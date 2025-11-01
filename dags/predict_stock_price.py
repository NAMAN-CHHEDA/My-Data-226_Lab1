
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

def _sf_conn():
    hook = SnowflakeHook(snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID", "snowflake_conn"))
    conn = hook.get_conn()
    try:
        conn.autocommit = False
    except Exception:
        pass
    return conn

def _get_context_sql():
    db = Variable.get("snowflake_database", default_var=None)
    wh = Variable.get("snowflake_warehouse", default_var=None)
    role = Variable.get("role", default_var=None)
    sqls = []
    if role:
        sqls.append(f"USE ROLE {role}")
    if wh:
        sqls.append(f"USE WAREHOUSE {wh}")
    if db:
        sqls.append(f"USE DATABASE {db}")
    return sqls

with DAG(
    dag_id="stock_forecast_ml",
    start_date=datetime(2025, 9, 25),
    schedule=None,  # triggered by ingest DAG
    catchup=False,
    tags=["Data226","stocks","snowflake","ml","forecast"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task()
    def ensure_objects():
        schema_raw = Variable.get("SCHEMA_RAW", "RAW")
        schema_analytics = Variable.get("SCHEMA_ANALYTICS", "ANALYTICS")
        target_col = Variable.get("FORECAST_TARGET_COL", "CLOSE")

        ddl_sqls = [
            f"CREATE SCHEMA IF NOT EXISTS {schema_analytics}",
            f"""
            CREATE OR REPLACE VIEW {schema_analytics}.V_STOCK_CLOSE_TS AS
            SELECT
              SYMBOL AS SERIES,                     -- use STRING directly for multi-series
              TO_TIMESTAMP_NTZ(DATE) AS TS,
              {target_col}::FLOAT AS TARGET
            FROM {schema_raw}.STOCK_PRICES
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema_analytics}.STOCK_FORECAST_7D (
              SYMBOL STRING,
              DATE   DATE,
              FORECAST FLOAT,
              LOWER_BOUND FLOAT,
              UPPER_BOUND FLOAT
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema_analytics}.STOCK_PRICES_FINAL (
              SYMBOL STRING,
              DATE   DATE,
              OPEN   FLOAT,
              HIGH   FLOAT,
              LOW    FLOAT,
              CLOSE  FLOAT,
              VOLUME NUMBER,
              FORECAST_CLOSE FLOAT,
              LOWER_BOUND FLOAT,
              UPPER_BOUND FLOAT,
              SOURCE STRING
            )
            """,
        ]

        conn = _sf_conn()
        cur = conn.cursor()
        try:
            for s in _get_context_sql():
                cur.execute(s)
            cur.execute("BEGIN")
            for stmt in ddl_sqls:
                cur.execute(stmt)
            cur.execute("COMMIT")
            return f"Ensured objects in {schema_analytics}"
        except Exception:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            cur.close()
            conn.close()

    @task()
    def train_model():
        schema_analytics = Variable.get("SCHEMA_ANALYTICS", "ANALYTICS")
        model_name = Variable.get("FORECAST_MODEL_NAME", "STOCK_CLOSE_MODEL")

        sql_train = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {schema_analytics}.{model_name} (
          INPUT_DATA        => TABLE({schema_analytics}.V_STOCK_CLOSE_TS),
          SERIES_COLNAME    => 'SERIES',
          TIMESTAMP_COLNAME => 'TS',
          TARGET_COLNAME    => 'TARGET',
          CONFIG_OBJECT     => OBJECT_CONSTRUCT('method','fast','frequency','1 day','evaluate',true)
        )
        """

        conn = _sf_conn()
        cur = conn.cursor()
        try:
            for s in _get_context_sql():
                cur.execute(s)
            cur.execute("BEGIN")
            cur.execute(sql_train)
            cur.execute("COMMIT")
            return "Model trained (or replaced)"
        except Exception:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            cur.close()
            conn.close()

    @task()
    def forecast_and_union():
        schema_raw = Variable.get("SCHEMA_RAW", "RAW")
        schema_analytics = Variable.get("SCHEMA_ANALYTICS", "ANALYTICS")
        model_name = Variable.get("FORECAST_MODEL_NAME", "STOCK_CLOSE_MODEL")

        conn = _sf_conn()
        cur = conn.cursor()
        try:
            for s in _get_context_sql():
                cur.execute(s)
            cur.execute("BEGIN")

            # refresh 7-day forecast
            cur.execute(f"TRUNCATE TABLE {schema_analytics}.STOCK_FORECAST_7D")
            cur.execute(f"""
                INSERT INTO {schema_analytics}.STOCK_FORECAST_7D (SYMBOL, DATE, FORECAST, LOWER_BOUND, UPPER_BOUND)
                SELECT
                  SERIES AS SYMBOL,
                  CAST(TS AS DATE) AS DATE,
                  FORECAST, LOWER_BOUND, UPPER_BOUND
                FROM TABLE({schema_analytics}.{model_name}!FORECAST(FORECASTING_PERIODS => 7))
            """)

            # rebuild final union table atomically
            cur.execute(f"""
                CREATE OR REPLACE TABLE {schema_analytics}.STOCK_PRICES_FINAL AS
                SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME,
                       NULL::FLOAT AS FORECAST_CLOSE, NULL::FLOAT AS LOWER_BOUND, NULL::FLOAT AS UPPER_BOUND,
                       'ACTUAL' AS SOURCE
                FROM {schema_raw}.STOCK_PRICES
                UNION ALL
                SELECT SYMBOL, DATE,
                       NULL::FLOAT AS OPEN, NULL::FLOAT AS HIGH, NULL::FLOAT AS LOW, NULL::FLOAT AS CLOSE, NULL::NUMBER AS VOLUME,
                       FORECAST AS FORECAST_CLOSE, LOWER_BOUND, UPPER_BOUND,
                       'FORECAST' AS SOURCE
                FROM {schema_analytics}.STOCK_FORECAST_7D
            """)

            cur.execute("COMMIT")
            return "Forecasted 7 days and rebuilt FINAL table"
        except Exception:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            cur.close()
            conn.close()

    _ensure = ensure_objects()
    _train  = train_model()
    _final  = forecast_and_union()

    _ensure >> _train >> _final
