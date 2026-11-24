# Guía de implementación — Adaptador EMR para ingestión Raw

> Guía de aprendizaje por tareas para construir `raw_emr_adapter.py`.
> Tú programas; los bloques de código son **esqueletos de base** con `TODO` y pistas de
> sintaxis/métodos, no implementaciones completas.

## Contexto

- **Objetivo:** adaptador que corre sobre Amazon EMR usando PySpark + JDBC para leer de un
  PostgreSQL en RDS, reutilizando el core agnóstico `run_raw_ingestion` y el
  `SsmWatermarkStore` existente.
- **Alcance:** procesa **una entidad por invocación** (igual que `raw_glue_adapter.py`). El
  fan-out a todas las entidades lo hace el orquestador vía EMR steps (fuera de esta guía).
- **Decisiones acordadas:**
  - Config en capas: **YAML** (estable) + **flags CLI** (variable por corrida) + **.env** (secretos).
  - `run-id` lo inyecta el orquestador; el adaptador lo recibe y falla si falta.
  - Driver JDBC provisto al cluster vía `--jars` desde S3 (subred privada + NAT). El adaptador
    asume que `org.postgresql.Driver` está en el classpath.
  - Watermark en SSM reutilizando `SsmWatermarkStore` (aislamiento por entidad, concurrencia
    optimista, no-retroceso).
  - Archivo: `scripts/raw/raw_emr_adapter.py`, junto al de Glue.

## El contrato del core (lo que NO reimplementas)

`run_raw_ingestion(...)` es agnóstico del motor. Te pide inyectar un `source_reader`:

```python
# Firma relevante del core (ya existe en raw_ingestion_core.py)
SourceReader = Callable[[str, str, str], Any]  # (sample_query, source_table, entity_name) -> DataFrame

def run_raw_ingestion(
    *,
    source_reader: SourceReader,
    target_entity: str,
    source_schema: str,
    source_system: str,
    s3_target_path: str,
    run_id: str,
    ingestion_mode: str,          # "initial" | "incremental"
    ingested_at: datetime,
    lower_bound: datetime,
    upper_bound: datetime,
) -> IngestionResult: ...
```

El core construye el query incremental, normaliza columnas, añade metadatos y escribe Parquet
particionado por `_ingestion_date` y `_run_id`. Tu adaptador solo aporta el **cómo leer** (JDBC)
y la **orquestación del watermark**.

Helpers ya disponibles en `common_libs.utils` (no los reimplementes):
`validate_run_id`, `validate_source_system`, `validate_s3_uri`, `parse_utc_timestamp`,
`format_utc_timestamp`, `qualified_table_name`.

---

## Task 1 — Esqueleto del adaptador y parsing de CLI

**Objetivo:** `main()` que parsee `--target-entity`, `--run-id`, `--config` y falle con mensaje
claro si falta alguno. Validar la entidad temprano.

**Métodos/sintaxis a usar:** `argparse.ArgumentParser`, `add_argument(..., required=True)`,
`get_entity_config` (lanza `ValueError` si la entidad no existe), `validate_run_id`.

```python
import argparse
import sys

from common_libs.schema_registry import get_entity_config
from common_libs.utils import validate_run_id


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Raw ingestion EMR adapter")
    parser.add_argument("--target-entity", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--config", required=True, help="Ruta al YAML de config")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    # TODO: validar entidad -> entity_config = get_entity_config(args.target_entity)
    # TODO: validar run_id  -> validate_run_id(args.run_id)
    # TODO: continuar con Task 2 (cargar config)
    ...


if __name__ == "__main__":
    main()
```

**Prueba:** `python raw_emr_adapter.py --target-entity books --run-id test1 --config x.yaml`
debe parsear; omitir un flag debe fallar con exit code != 0.

---

## Task 2 — Carga de configuración (YAML + .env)

**Objetivo:** leer el YAML (host, port, db, schema, source_system, s3_target_path,
ssm_watermark_prefix, initial_watermark) y credenciales desde `.env`. Nunca loguear el password.

**Métodos/sintaxis:** `yaml.safe_load`, `dotenv.load_dotenv`, `os.environ`, `@dataclass`.

### Esquema del YAML (`raw.prod.yaml` de ejemplo)

```yaml
# Parámetros estables del pipeline/entorno (versionable, SIN secretos)
rds:
  host: mi-rds.xxxxxx.us-east-1.rds.amazonaws.com
  port: 5432
  database: bookstore
source_schema: public
source_system: bookstore_oltp
s3_target_path: s3://mi-bucket/raw
ssm_watermark_prefix: /bookstore/raw/watermark
initial_watermark: "1970-01-01T00:00:00Z"
```

### Esquema del `.env` de ejemplo (NO versionar)

```dotenv
RDS_USER=ingestion_user
RDS_PASSWORD=super-secreto
```

### Esqueleto de carga

```python
import os
from dataclasses import dataclass

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class AdapterConfig:
    host: str
    port: int
    database: str
    source_schema: str
    source_system: str
    s3_target_path: str
    ssm_watermark_prefix: str
    initial_watermark: str
    db_user: str
    db_password: str  # cuidado: no imprimir


def load_config(config_path: str) -> AdapterConfig:
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    load_dotenv()  # lee .env del cwd; en EMR puedes apuntar a una ruta explícita
    user = os.environ["RDS_USER"]          # KeyError si falta -> falla temprano
    password = os.environ["RDS_PASSWORD"]

    # TODO: mapear 'raw' -> AdapterConfig y validar campos obligatorios
    # TODO: castear port a int; validar s3_target_path con validate_s3_uri
    ...
```

**Prueba:** cargar YAML + `.env` de ejemplo y verificar que todos los campos llegan; falta de un
campo obligatorio debe fallar.

---

## Task 3 — SparkSession y lector JDBC

**Objetivo:** crear la `SparkSession` y `read_postgres_query(...)` que devuelva un DataFrame.

**Métodos/sintaxis:** `SparkSession.builder.appName(...).getOrCreate()`,
`spark.conf.set("spark.sql.session.timeZone", "UTC")`, `spark.read.format("jdbc")`.

```python
from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # igual que el Glue adapter
    return spark


def build_jdbc_url(host: str, port: int, database: str) -> str:
    # jdbc:postgresql://host:port/database
    return f"jdbc:postgresql://{host}:{port}/{database}"


def read_postgres_query(spark, *, jdbc_url: str, user: str, password: str, sample_query: str):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", sample_query)       # usa 'query' (no 'dbtable') para SELECTs arbitrarios
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
```

> Nota sobre el driver: `org.postgresql.Driver` debe estar en el classpath. Llega vía
> `--jars s3://.../postgresql-<ver>.jar` en el `spark-submit`. El código Python NO lo instala.

**Prueba:** contra un Postgres de prueba, leer `SELECT 1` y mostrar el DataFrame. Al menos
verificar que `build_jdbc_url` arma bien la URL.

---

## Task 4 — Cálculo de `upper_bound` e `ingestion_mode`

**Objetivo:** replicar la lógica del Glue adapter: `upper_bound` = `SELECT CURRENT_TIMESTAMP`
del RDS; leer el watermark con `SsmWatermarkStore.read`; decidir modo; validar que el watermark
almacenado no sea posterior al upper_bound.

**Métodos/sintaxis:** `read_postgres_query(...)` con el query de CURRENT_TIMESTAMP, `.limit(2).collect()`,
`parse_utc_timestamp(..., assume_naive_utc=True)`, `boto3.client("ssm")`, `SsmWatermarkStore`.

```python
from datetime import datetime, timezone

import boto3

from common_libs.utils import parse_utc_timestamp
from common_libs.watermark_store import SsmWatermarkStore


def read_database_upper_bound(spark, *, jdbc_url, user, password, entity_name) -> datetime:
    df = read_postgres_query(
        spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        sample_query="SELECT CURRENT_TIMESTAMP AS current_run_upper_bound",
    )
    rows = df.limit(2).collect()
    # TODO: exigir exactamente 1 fila y valor no-nulo; si no, RuntimeError
    return parse_utc_timestamp(rows[0]["current_run_upper_bound"], assume_naive_utc=True)


def resolve_watermark(entity_name: str, cfg) :
    store = SsmWatermarkStore(boto3.client("ssm"), cfg.ssm_watermark_prefix)
    state = store.read(entity_name, cfg.initial_watermark)
    # ingestion_mode = "incremental" if state.exists else "initial"
    # TODO: validar state.value <= upper_bound (si no, RuntimeError de clocks/config)
    return store, state
```

**Prueba:** mockear el lector (timestamp fijo) y el cliente SSM; verificar decisión de modo y
la validación "watermark > upper_bound".

---

## Task 5 — Integración con el core y avance del watermark

**Objetivo:** definir el closure `source_reader`, llamar a `run_raw_ingestion(...)` y luego
`watermark_store.advance(...)`. Manejar `WatermarkConflictError`.

**Punto clave:** el orden importa — leer → escribir (dentro del core) → `advance`. Si falla antes
del `advance`, el watermark NO se mueve y el reintento reprocesa la ventana.

```python
from datetime import datetime, timezone

from common_libs.watermark_store import WatermarkConflictError
from raw_ingestion_core import run_raw_ingestion


def run(cfg, entity_config, run_id, spark, jdbc_url):
    store, watermark_state = resolve_watermark(entity_config.name, cfg)
    upper_bound = read_database_upper_bound(
        spark, jdbc_url=jdbc_url, user=cfg.db_user, password=cfg.db_password,
        entity_name=entity_config.name,
    )
    ingestion_mode = "incremental" if watermark_state.exists else "initial"
    ingested_at = datetime.now(timezone.utc)

    def source_reader(sample_query, source_table, entity_name):
        # closure: captura spark/jdbc_url/credenciales
        return read_postgres_query(
            spark, jdbc_url=jdbc_url, user=cfg.db_user,
            password=cfg.db_password, sample_query=sample_query,
        )

    result = run_raw_ingestion(
        source_reader=source_reader,
        target_entity=entity_config.name,
        source_schema=cfg.source_schema,
        source_system=cfg.source_system,
        s3_target_path=cfg.s3_target_path,
        run_id=run_id,
        ingestion_mode=ingestion_mode,
        ingested_at=ingested_at,
        lower_bound=watermark_state.value,
        upper_bound=upper_bound,
    )

    try:
        updated = store.advance(entity_config.name, watermark_state, upper_bound)
    except WatermarkConflictError:
        # TODO: log claro; otra corrida de la MISMA entidad avanzó el watermark
        raise
    return result, updated
```

**Prueba:** integración con mocks del lector (DataFrame en memoria) y del SSM; verificar que
`advance` se llama solo tras el core y con los valores correctos.

---

## Task 6 — Logging estructurado y manejo de errores

**Objetivo:** `log_event(...)` en JSON (equivalente al Glue adapter) para `raw_ingestion_started`
y `raw_ingestion_completed`; `sys.exit(1)` ante error para que el EMR step se marque fallido.

```python
import json
import sys
from datetime import datetime, timezone

from common_libs.utils import format_utc_timestamp


def log_event(event: str, **fields) -> None:
    payload = {
        "event": event,
        "timestamp": format_utc_timestamp(datetime.now(timezone.utc)),
        **fields,
    }
    print(json.dumps(payload, sort_keys=True, default=str))


def main() -> None:
    try:
        # TODO: parse_args -> load_config -> build_spark -> run(...)
        # log_event("raw_ingestion_started", entity=..., ingestion_mode=..., run_id=...)
        # log_event("raw_ingestion_completed", entity=..., row_count=..., ...)
        ...
    except Exception as exc:  # EMR marca el step como fallido con exit != 0
        log_event("raw_ingestion_failed", error=str(exc))
        sys.exit(1)
```

**Prueba:** capturar stdout y verificar eventos; verificar exit code != 0 en error simulado.

---

## Task 7 — Empaquetado y ejecución en EMR

**Objetivo:** documentar el lanzamiento y las dependencias.

- **Dependencias Python:** `pyyaml`, `python-dotenv` (instalar vía bootstrap action o
  `--archives`/venv empaquetado).
- **Driver JDBC:** `postgresql-<ver>.jar` en S3; un bootstrap action lo copia al cluster, y se
  pasa con `--jars`.
- **Common libs:** empaquetadas en `common_libs.zip` y pasadas con `--py-files`.

### Comando `spark-submit` de ejemplo (un step por entidad)

```bash
RUN_ID="20240115T020000Z"   # el orquestador lo genera UNA vez por corrida

spark-submit \
  --jars s3://mi-bucket/jars/postgresql-42.7.3.jar \
  --py-files s3://mi-bucket/libs/common_libs.zip,s3://mi-bucket/scripts/common/raw_ingestion_core.py \
  s3://mi-bucket/scripts/raw/raw_emr_adapter.py \
  --config s3://mi-bucket/config/raw.prod.yaml \
  --target-entity books \
  --run-id "$RUN_ID"
```

### Notas de orquestación (fan-out con EMR steps)

- Un step por entidad (`books`, `customers`, `orders`, `order_items`), **mismo `run_id`**.
- Para correr en paralelo: habilitar **step concurrency** en el cluster y configurar
  `ActionOnFailure` de modo que el fallo de un step **no cancele** los demás.
- **Aislamiento por watermark:** cada entidad tiene su parámetro SSM
  (`<prefix>/<entity>`), así que si una entidad falla y otras no, solo la fallida queda sin
  avanzar su watermark. El reintento de esa entidad reprocesa su ventana; las demás no se tocan.
- La capa aguas abajo deduplica usando `_record_hash` y `_run_id` (Raw es append-only).

---

## Orden sugerido de trabajo

1. Task 1 y 2 (mecánicas) → pídeme editarlas cuando las entiendas.
2. Task 3 y 5 (núcleo de aprendizaje: JDBC + closure del `source_reader`) → hazlas tú, yo te guío.
3. Task 4 (watermark + upper_bound).
4. Task 6 y 7 (logging, empaquetado, docs).
