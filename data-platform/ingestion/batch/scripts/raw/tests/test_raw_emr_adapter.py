import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import raw_emr_adapter
from common_libs.watermark_store import WatermarkConflictError
from raw_ingestion_core import IngestionResult


SAMPLE_YAML = """
rds:
  host: bookstore-db.xxxx.us-east-1.rds.amazonaws.com
  port: 5432
  database: bookstore
source_schema: public
source_system: bookstore_oltp
s3_target_path: s3://bookstore-raw-bucket/raw
ssm_watermark_prefix: /bookstore/raw/watermark
initial_watermark: "1970-01-01T00:00:00Z"
"""


class FakeSsmClient:
    def __init__(self, initial_parameters=None):
        self.parameters = dict(initial_parameters or {})
        self.put_calls = []

    def get_parameter(self, *, Name, WithDecryption=False):
        if Name not in self.parameters:
            exc = Exception("ParameterNotFound")
            exc.response = {"Error": {"Code": "ParameterNotFound"}}
            raise exc
        return {"Parameter": {"Name": Name, "Value": self.parameters[Name]}}

    def put_parameter(self, *, Name, Value, Type, Overwrite, Description):
        self.parameters[Name] = Value
        self.put_calls.append(
            {
                "Name": Name,
                "Value": Value,
                "Type": Type,
                "Overwrite": Overwrite,
                "Description": Description,
            }
        )


class TestRawEmrAdapter(unittest.TestCase):
    def setUp(self):
        self.env_patcher = patch.dict(
            os.environ,
            {"RDS_USER": "bookstore_ingest", "RDS_PASSWORD": "secret_password_123"},
            clear=False,
        )
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()

    def test_parse_args_success(self):
        args = raw_emr_adapter.parse_args(
            [
                "--target-entity",
                "books",
                "--run-id",
                "airflow_run_20261124",
                "--config",
                "s3://my-bucket/config/raw.prod.yaml",
            ]
        )
        self.assertEqual(args.target_entity, "books")
        self.assertEqual(args.run_id, "airflow_run_20261124")
        self.assertEqual(args.config, "s3://my-bucket/config/raw.prod.yaml")

    def test_parse_args_missing_required_flag_exits(self):
        with self.assertRaises(SystemExit):
            raw_emr_adapter.parse_args(["--target-entity", "books", "--run-id", "run1"])

    def test_load_config_from_local_file_and_hide_password_in_repr(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            tmp.write(SAMPLE_YAML)
            tmp_path = tmp.name

        try:
            cfg = raw_emr_adapter.load_config(tmp_path)
            self.assertEqual(cfg.host, "bookstore-db.xxxx.us-east-1.rds.amazonaws.com")
            self.assertEqual(cfg.port, 5432)
            self.assertEqual(cfg.database, "bookstore")
            self.assertEqual(cfg.source_schema, "public")
            self.assertEqual(cfg.source_system, "bookstore_oltp")
            self.assertEqual(cfg.s3_target_path, "s3://bookstore-raw-bucket/raw")
            self.assertEqual(cfg.ssm_watermark_prefix, "/bookstore/raw/watermark")
            self.assertEqual(cfg.initial_watermark, "1970-01-01T00:00:00Z")
            self.assertEqual(cfg.db_user, "bookstore_ingest")
            self.assertEqual(cfg.db_password, "secret_password_123")
            self.assertEqual(
                cfg.jdbc_url(),
                "jdbc:postgresql://bookstore-db.xxxx.us-east-1.rds.amazonaws.com:5432/bookstore",
            )
            self.assertNotIn("secret_password_123", repr(cfg))
        finally:
            os.remove(tmp_path)

    def test_load_config_from_s3_uri(self):
        fake_s3 = MagicMock()
        fake_s3.get_object.return_value = {
            "Body": io.BytesIO(SAMPLE_YAML.encode("utf-8"))
        }

        cfg = raw_emr_adapter.load_config(
            "s3://my-config-bucket/configs/raw.prod.yaml",
            s3_client=fake_s3,
        )
        fake_s3.get_object.assert_called_once_with(
            Bucket="my-config-bucket",
            Key="configs/raw.prod.yaml",
        )
        self.assertEqual(cfg.database, "bookstore")

    def test_load_config_fails_when_credentials_missing(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            tmp.write(SAMPLE_YAML)
            tmp_path = tmp.name

        try:
            with patch.dict(os.environ, {"RDS_USER": "", "RDS_PASSWORD": ""}, clear=False):
                with patch("raw_emr_adapter.load_dotenv"):
                    with self.assertRaisesRegex(ValueError, "RDS_USER"):
                        raw_emr_adapter.load_config(tmp_path)
        finally:
            os.remove(tmp_path)

    def test_read_postgres_query_uses_spark_jdbc_options(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        expected_df = MagicMock()
        mock_reader.load.return_value = expected_df

        df = raw_emr_adapter.read_postgres_query(
            mock_spark,
            jdbc_url="jdbc:postgresql://host:5432/bookstore",
            user="bookstore_ingest",
            password="secret",
            sample_query='SELECT "book_id" FROM "public"."books"',
        )

        self.assertIs(df, expected_df)
        mock_spark.read.format.assert_called_once_with("jdbc")
        mock_reader.option.assert_any_call("url", "jdbc:postgresql://host:5432/bookstore")
        mock_reader.option.assert_any_call("query", 'SELECT "book_id" FROM "public"."books"')
        mock_reader.option.assert_any_call("user", "bookstore_ingest")
        mock_reader.option.assert_any_call("password", "secret")
        mock_reader.option.assert_any_call("driver", "org.postgresql.Driver")
        mock_reader.load.assert_called_once()

    def test_read_database_upper_bound_returns_utc_datetime(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.limit.return_value.collect.return_value = [
            {"current_run_upper_bound": datetime(2026, 11, 24, 13, 30, 0)}
        ]

        with patch("raw_emr_adapter.read_postgres_query", return_value=mock_df) as mock_read:
            upper = raw_emr_adapter.read_database_upper_bound(
                mock_spark,
                jdbc_url="jdbc:postgresql://host:5432/bookstore",
                user="u",
                password="p",
                target_entity="books",
            )
            self.assertEqual(upper, datetime(2026, 11, 24, 13, 30, 0, tzinfo=timezone.utc))
            mock_read.assert_called_once_with(
                mock_spark,
                jdbc_url="jdbc:postgresql://host:5432/bookstore",
                user="u",
                password="p",
                sample_query="SELECT CURRENT_TIMESTAMP AS current_run_upper_bound",
            )

    def test_read_database_upper_bound_raises_on_invalid_rows(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.limit.return_value.collect.return_value = []

        with patch("raw_emr_adapter.read_postgres_query", return_value=mock_df):
            with self.assertRaises(RuntimeError):
                raw_emr_adapter.read_database_upper_bound(
                    mock_spark,
                    jdbc_url="jdbc:postgresql://host:5432/bookstore",
                    user="u",
                    password="p",
                    target_entity="books",
                )

    def test_run_job_initial_and_incremental_flow(self):
        cfg = raw_emr_adapter.AdapterConfig(
            host="host",
            port=5432,
            database="bookstore",
            source_schema="public",
            source_system="bookstore_oltp",
            s3_target_path="s3://bucket/raw",
            ssm_watermark_prefix="/bookstore/raw/watermark",
            initial_watermark="1970-01-01T00:00:00Z",
            db_user="bookstore_ingest",
            db_password="secret",
        )
        fake_ssm = FakeSsmClient()
        mock_spark = MagicMock()
        upper_bound = datetime(2026, 11, 24, 14, 0, 0, tzinfo=timezone.utc)

        captured_core_kwargs = {}

        def fake_core(**kwargs):
            captured_core_kwargs.update(kwargs)
            # Also invoke the injected source_reader closure to verify it delegates to JDBC
            kwargs["source_reader"](
                'SELECT "book_id" FROM "public"."books"',
                "public.books",
                "books",
            )
            return IngestionResult(
                entity_name="books",
                row_count=42,
                target_path="s3://bucket/raw/books/",
                lower_bound=kwargs["lower_bound"],
                upper_bound=kwargs["upper_bound"],
                run_id=kwargs["run_id"],
            )

        with patch("raw_emr_adapter.read_database_upper_bound", return_value=upper_bound), patch(
            "raw_emr_adapter.read_postgres_query"
        ) as mock_jdbc_read, patch("raw_emr_adapter.run_raw_ingestion", side_effect=fake_core):
            buf = io.StringIO()
            with redirect_stdout(buf):
                result = raw_emr_adapter.run_job(
                    target_entity="books",
                    run_id="run_001",
                    config=cfg,
                    spark=mock_spark,
                    ssm_client=fake_ssm,
                )

        self.assertEqual(result.row_count, 42)
        self.assertEqual(captured_core_kwargs["ingestion_mode"], "initial")
        self.assertEqual(
            captured_core_kwargs["lower_bound"],
            datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(captured_core_kwargs["upper_bound"], upper_bound)
        mock_jdbc_read.assert_called_once_with(
            mock_spark,
            jdbc_url="jdbc:postgresql://host:5432/bookstore",
            user="bookstore_ingest",
            password="secret",
            sample_query='SELECT "book_id" FROM "public"."books"',
        )
        self.assertEqual(
            fake_ssm.parameters["/bookstore/raw/watermark/books"],
            "2026-11-24T14:00:00.000000Z",
        )

        events = [json.loads(line) for line in buf.getvalue().strip().splitlines()]
        self.assertEqual(events[0]["event"], "raw_ingestion_started")
        self.assertEqual(events[0]["ingestion_mode"], "initial")
        self.assertEqual(events[1]["event"], "raw_ingestion_completed")
        self.assertEqual(events[1]["row_count"], 42)
        self.assertTrue(events[1]["watermark_updated"])

    def test_run_job_aborts_if_stored_watermark_is_after_upper_bound(self):
        cfg = raw_emr_adapter.AdapterConfig(
            host="host",
            port=5432,
            database="bookstore",
            source_schema="public",
            source_system="bookstore_oltp",
            s3_target_path="s3://bucket/raw",
            ssm_watermark_prefix="/bookstore/raw/watermark",
            initial_watermark="1970-01-01T00:00:00Z",
            db_user="bookstore_ingest",
            db_password="secret",
        )
        fake_ssm = FakeSsmClient(
            {"/bookstore/raw/watermark/books": "2026-12-01T00:00:00.000000Z"}
        )
        upper_bound = datetime(2026, 11, 24, 14, 0, 0, tzinfo=timezone.utc)

        with patch("raw_emr_adapter.read_database_upper_bound", return_value=upper_bound), patch(
            "raw_emr_adapter.run_raw_ingestion"
        ) as mock_core:
            with self.assertRaisesRegex(RuntimeError, "Stored watermark is later"):
                raw_emr_adapter.run_job(
                    target_entity="books",
                    run_id="run_002",
                    config=cfg,
                    spark=MagicMock(),
                    ssm_client=fake_ssm,
                )
            mock_core.assert_not_called()

    def test_main_logs_failure_and_exits_with_code_1_on_error(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            with self.assertRaises(SystemExit) as cm:
                raw_emr_adapter.main(
                    [
                        "--target-entity",
                        "unsupported_table",
                        "--run-id",
                        "run_003",
                        "--config",
                        "nonexistent.yaml",
                    ]
                )
        self.assertEqual(cm.exception.code, 1)
        payload = json.loads(buf.getvalue().strip())
        self.assertEqual(payload["event"], "raw_ingestion_failed")
        self.assertIn("Unsupported entity", payload["error"])


if __name__ == "__main__":
    unittest.main()
