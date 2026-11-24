"""
Airflow DAG: BookStore Raw Ingestion on Ephemeral Amazon EMR Cluster.

This DAG implements the transient EMR cluster pattern:
1. Creates an Amazon EMR cluster (Spark) inside the private subnet.
2. Submits one PySpark step per entity (books, customers, orders, order_items)
   invoking raw_emr_adapter.py with a shared --run-id (`airflow_{{ ts_nodash }}`).
3. Monitors each step until completion.
4. Always terminates the EMR cluster (`trigger_rule=TriggerRule.ALL_DONE`), even
   if one or more entity steps fail, preventing unexpected AWS charges.
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Environment / Pipeline Configuration
# ---------------------------------------------------------------------------
AWS_CONN_ID = os.environ.get("AWS_CONN_ID", "aws_default")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

BUCKET_NAME = os.environ.get("BOOKSTORE_BUCKET_NAME", "replace-with-datalake-bucket")
EMR_SUBNET_ID = os.environ.get("EMR_SUBNET_ID", "subnet-replace-with-private-1a")
EMR_ADDITIONAL_SG_ID = os.environ.get("EMR_ADDITIONAL_SG_ID", "sg-replace-with-rds-sg")
EMR_KEY_NAME = os.environ.get("EMR_KEY_NAME", "bookstore-key")

EMR_SERVICE_ROLE = os.environ.get("EMR_SERVICE_ROLE", "EMR_DefaultRole")
EMR_INSTANCE_PROFILE = os.environ.get("EMR_INSTANCE_PROFILE", "EMR_EC2_DefaultRole")
EMR_RELEASE_LABEL = os.environ.get("EMR_RELEASE_LABEL", "emr-7.2.0")
EMR_MASTER_INSTANCE_TYPE = os.environ.get("EMR_MASTER_INSTANCE_TYPE", "m5.xlarge")
EMR_CORE_INSTANCE_TYPE = os.environ.get("EMR_CORE_INSTANCE_TYPE", "m5.xlarge")

RDS_USER = os.environ.get("RDS_USER", "bookstore_ingest")
RDS_PASSWORD = os.environ.get("RDS_PASSWORD", "Passwd123456!*")

# Default schedule: every 6 hours (can be overridden via BOOKSTORE_INGEST_SCHEDULE env var)
SCHEDULE_INTERVAL = os.environ.get("BOOKSTORE_INGEST_SCHEDULE", "0 */6 * * *")

ENTITIES: List[str] = ["books", "customers", "orders", "order_items"]

# Matches validate_run_id() regex: ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$
RUN_ID_TEMPLATE = "airflow_{{ ts_nodash }}"


def build_job_flow_overrides() -> Dict[str, Any]:
    """Build the Amazon EMR RunJobFlow payload for an ephemeral Spark cluster."""
    instances_config: Dict[str, Any] = {
        "InstanceGroups": [
            {
                "Name": "MasterNode",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": EMR_MASTER_INSTANCE_TYPE,
                "InstanceCount": 1,
            },
            {
                "Name": "CoreNodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": EMR_CORE_INSTANCE_TYPE,
                "InstanceCount": 1,
            },
        ],
        "Ec2SubnetId": EMR_SUBNET_ID,
        "AdditionalMasterSecurityGroups": [EMR_ADDITIONAL_SG_ID],
        "AdditionalSlaveSecurityGroups": [EMR_ADDITIONAL_SG_ID],
        # Keep alive while Airflow adds and monitors steps; Airflow + AutoTerminationPolicy will terminate it
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    }
    if EMR_KEY_NAME:
        instances_config["Ec2KeyName"] = EMR_KEY_NAME

    return {
        "Name": f"bookstore-emr-raw-{RUN_ID_TEMPLATE}",
        "LogUri": f"s3://{BUCKET_NAME}/logs/emr/",
        "ReleaseLabel": EMR_RELEASE_LABEL,
        "Applications": [{"Name": "Spark"}],
        "Instances": instances_config,
        "BootstrapActions": [
            {
                "Name": "InstallPythonDepsAndEnv",
                "ScriptBootstrapAction": {
                    "Path": f"s3://{BUCKET_NAME}/scripts/bootstrap/emr_bootstrap.sh",
                },
            }
        ],
        "StepConcurrencyLevel": 4,
        # Safety net: if Airflow crashes, EMR auto-terminates after 15 minutes idle
        "AutoTerminationPolicy": {"IdleTimeout": 900},
        "JobFlowRole": EMR_INSTANCE_PROFILE,
        "ServiceRole": EMR_SERVICE_ROLE,
        "VisibleToAllUsers": True,
        "Tags": [
            {"Key": "Project", "Value": "bookstore"},
            {"Key": "Layer", "Value": "raw-ingestion"},
            {"Key": "Orchestrator", "Value": "airflow"},
        ],
    }


def build_entity_spark_step(entity_name: str, run_id: str) -> List[Dict[str, Any]]:
    """Build the EMR step definition that executes raw_emr_adapter.py for one entity."""
    return [
        {
            "Name": f"raw_ingestion_{entity_name}_{run_id}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "env",
                    f"RDS_USER={RDS_USER}",
                    f"RDS_PASSWORD={RDS_PASSWORD}",
                    f"AWS_DEFAULT_REGION={AWS_REGION}",
                    f"AWS_REGION={AWS_REGION}",
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    "--conf",
                    f"spark.yarn.appMasterEnv.RDS_USER={RDS_USER}",
                    "--conf",
                    f"spark.yarn.appMasterEnv.RDS_PASSWORD={RDS_PASSWORD}",
                    "--conf",
                    f"spark.executorEnv.RDS_USER={RDS_USER}",
                    "--conf",
                    f"spark.executorEnv.RDS_PASSWORD={RDS_PASSWORD}",
                    "--jars",
                    f"s3://{BUCKET_NAME}/jars/postgresql-42.7.3.jar",
                    "--py-files",
                    (
                        f"s3://{BUCKET_NAME}/libs/common_libs.zip,"
                        f"s3://{BUCKET_NAME}/scripts/common/raw_ingestion_core.py"
                    ),
                    f"s3://{BUCKET_NAME}/scripts/raw/raw_emr_adapter.py",
                    "--config",
                    f"s3://{BUCKET_NAME}/config/raw.prod.yaml",
                    "--target-entity",
                    entity_name,
                    "--run-id",
                    run_id,
                ],
            },
        }
    ]


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="bookstore_raw_emr_ingestion",
    description="Periodic multi-entity Raw ingestion from RDS PostgreSQL to S3 via ephemeral Amazon EMR",
    default_args=default_args,
    schedule=SCHEDULE_INTERVAL,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bookstore", "raw", "emr", "incremental"],
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=build_job_flow_overrides(),
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    for entity in ENTITIES:
        add_step = EmrAddStepsOperator(
            task_id=f"add_step_{entity}",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            aws_conn_id=AWS_CONN_ID,
            steps=build_entity_spark_step(entity, RUN_ID_TEMPLATE),
        )

        watch_step = EmrStepSensor(
            task_id=f"watch_step_{entity}",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_step_{entity}', key='return_value')[0] }}}}",
            aws_conn_id=AWS_CONN_ID,
            poke_interval=30,
            timeout=3600,
        )

        create_emr_cluster >> add_step >> watch_step >> terminate_emr_cluster
