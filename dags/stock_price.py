# dags/stock_ingest_yf.py
#
# Airflow DAG: Pull last N (default 180) days of OHLCV for 2+ tickers via yfinance
# and upsert into Snowflake RAW.STOCK_PRICES using a transactional MERGE.
#
# Cloud Composer: add these PyPI packages:
#   apache-airflow-providers-snowflake
#   yfinance
#
# Airflow Variables expected (with sensible defaults):
#   SNOWFLAKE_CONN_ID        -> default "snowflake_conn"
#   SNOWFLAKE_DATABASE       -> e.g., "FINANCE"
#   SNOWFLAKE_WAREHOUSE      -> e.g., "COMPUTE_WH"
#   SNOWFLAKE_ROLE           -> optional
#   SCHEMA_RAW               -> default "RAW"
#   LOOKBACK_DAYS            -> default "180"
#   STOCK_SYMBOLS_JSON       -> e.g., '["AAPL","NVDA"]'
#
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import json

def _sf_conn():
    hook = SnowflakeHook(snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID", "snowflake_conn"))
    conn = hook.get_conn()
    try:
        conn.autocommit = False  # explicitly control transactions
    except Exception:
        pass
    return conn

def _get_context_sql():
    db = Variable.get("SNOWFLAKE_DATABASE", default_var=None)
    wh = Variable.get("SNOWFLAKE_WAREHOUSE", default_var=None)
    role = Variable.get("SNOWFLAKE_ROLE", default_var=None)
    sqls = []
    if role:
        sqls.append(f"USE ROLE {role}")
    if wh:
        sqls.append(f"USE WAREHOUSE {wh}")
    if db:
        sqls.append(f"USE DATABASE {db}")
    return sqls

with DAG(
    dag_id="stock_ingest_yf",
    start_date=datetime(2025, 9, 25),
    schedule="15 2 * * *",  # 02:15 daily
    catchup=False,
    tags=["Data226","stocks","snowflake","yfinance"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task()
    def ensure_tables():
        schema_raw = Variable.get("SCHEMA_RAW", "RAW")
        table = f"{schema_raw}.STOCK_PRICES"
        staging = f"{schema_raw}.STOCK_PRICES_STG"

        ddl_main = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            SYMBOL STRING NOT NULL,
            DATE   DATE   NOT NULL,
            OPEN   FLOAT,
            HIGH   FLOAT,
            LOW    FLOAT,
            CLOSE  FLOAT,
            VOLUME NUMBER,
            CONSTRAINT PK_STOCK_PRICES PRIMARY KEY (SYMBOL, DATE)
        );
        """
        ddl_stg = f"CREATE TABLE IF NOT EXISTS {staging} LIKE {table};"

        conn = _sf_conn()
        cur = conn.cursor()
        try:
            for s in _get_context_sql():
                cur.execute(s)
            cur.execute(ddl_main)
            cur.execute(ddl_stg)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    @task()
    def fetch_prices() -> dict:
        """Fetch last LOOKBACK_DAYS for each symbol via yfinance and return dict {symbol: [rows]}."""
        lookback_days = int(Variable.get("LOOKBACK_DAYS", "180"))
        symbols_json = Variable.get("STOCK_SYMBOLS_JSON", '["IBM","CSCO","MSFT","AMZN","AAPL","NVDA"]')
        symbols = json.loads(symbols_json)

        out = {}
        for sym in symbols:
            hist = yf.Ticker(sym).history(
                period=f"{lookback_days+10}d",
                interval="1d",
                actions=False,
                auto_adjust=False
            )
            if hist is None or hist.empty:
                out[sym] = []
                continue

            hist = hist.dropna(subset=["Open","High","Low","Close","Volume"]).copy()
            hist.reset_index(inplace=True)  # 'Date' column appears
            hist["Date"] = pd.to_datetime(hist["Date"]).dt.date
            hist = hist.sort_values("Date").tail(lookback_days)

            rows = []
            for _, r in hist.iterrows():
                rows.append({
                    "SYMBOL": sym,
                    "DATE": str(r["Date"]),  # ISO date string; Snowflake will cast
                    "OPEN": float(r["Open"]),
                    "HIGH": float(r["High"]),
                    "LOW": float(r["Low"]),
                    "CLOSE": float(r["Close"]),
                    "VOLUME": int(r["Volume"]),
                })
            out[sym] = rows
        return out

    @task()
    def upsert_all(data_by_symbol: dict) -> str:
        """Transactional upsert into RAW.STOCK_PRICES via STAGING + MERGE."""
        schema_raw = Variable.get("SCHEMA_RAW", "RAW")
        table = f"{schema_raw}.STOCK_PRICES"
        staging = f"{schema_raw}.STOCK_PRICES_STG"

        if not data_by_symbol:
            return "No symbols"

        conn = _sf_conn()
        cur = conn.cursor()
        try:
            for s in _get_context_sql():
                cur.execute(s)
            cur.execute("BEGIN")
            cur.execute(f"TRUNCATE TABLE {staging}")

            ins_sql = f"""
                INSERT INTO {staging} (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
                VALUES (%(SYMBOL)s, %(DATE)s, %(OPEN)s, %(HIGH)s, %(LOW)s, %(CLOSE)s, %(VOLUME)s)
            """
            for sym, rows in data_by_symbol.items():
                if rows:
                    cur.executemany(ins_sql, rows)

            merge_sql = f"""
                MERGE INTO {table} t
                USING {staging} s
                ON t.SYMBOL = s.SYMBOL AND t.DATE = s.DATE
                WHEN MATCHED THEN UPDATE SET
                    t.OPEN = s.OPEN, t.HIGH = s.HIGH, t.LOW = s.LOW, t.CLOSE = s.CLOSE, t.VOLUME = s.VOLUME
                WHEN NOT MATCHED THEN INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
                VALUES (s.SYMBOL, s.DATE, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME);
            """
            cur.execute(merge_sql)
            cur.execute("COMMIT")
            return "Upserted RAW.STOCK_PRICES from yfinance"
        except Exception:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            cur.close()
            conn.close()

    ensure = ensure_tables()
    fetched = fetch_prices()
    loaded = upsert_all(fetched)

    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_forecast_dag",
        trigger_dag_id="stock_forecast_ml",
        wait_for_completion=False,
    )

    ensure >> fetched >> loaded >> trigger_forecast
