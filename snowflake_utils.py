import os
import pathlib
from typing import Any, Optional, Sequence, Tuple, Union

import pandas as pd

try:
    import streamlit as st  
except Exception:  
    st = None 

try:
    import snowflake.connector 
except Exception as exc:
    raise RuntimeError(
        "snowflake-connector-python is required. Install dependencies from requirements.txt"
    ) from exc


def _get_snowflake_config() -> dict:
    """Fetch Snowflake connection params from Streamlit secrets or environment variables."""
    cfg: dict
    if st is not None and hasattr(st, "secrets") and "snowflake" in st.secrets:
        cfg = dict(st.secrets["snowflake"])  
    else:
        cfg = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "user": os.getenv("SNOWFLAKE_USER", ""),
            "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
            "role": os.getenv("SNOWFLAKE_ROLE", ""),
            "database": os.getenv("SNOWFLAKE_DATABASE", "BOOTCAMP_RALLY"),
        }
    missing = [k for k, v in cfg.items() if k in ("account", "user", "password") and not v]
    if missing:
        raise ValueError(f"Missing Snowflake credentials: {', '.join(missing)}")
    return cfg


def get_connection() -> "snowflake.connector.SnowflakeConnection":
    cfg = _get_snowflake_config()
    conn = snowflake.connector.connect(
        account=cfg.get("account"),
        user=cfg.get("user"),
        password=cfg.get("password"),
        warehouse=cfg.get("warehouse") or None,
        role=cfg.get("role") or None,
        database=cfg.get("database") or None,
    )
    return conn


def execute(
    conn: "snowflake.connector.SnowflakeConnection",
    sql: str,
    params: Optional[Union[Sequence[Any], dict]] = None,
    fetch: str = "none",
) -> Union[None, Tuple, list, pd.DataFrame]:
    """Execute a SQL statement with optional parameters.

    - fetch='none' returns None
    - fetch='one' returns a single tuple or None
    - fetch='all' returns list of tuples
    - fetch='df' returns a pandas DataFrame
    """
    with conn.cursor() as cur:
        cur.execute(sql, params) if params else cur.execute(sql)
        if fetch == "none":
            return None
        if fetch == "one":
            row = cur.fetchone()
            return row
        if fetch == "all":
            rows = cur.fetchall()
            return rows
        if fetch == "df":
            try:
                return cur.fetch_pandas_all()
            except Exception:
                columns = [c[0] for c in cur.description]
                rows = cur.fetchall()
                return pd.DataFrame(rows, columns=columns)
        raise ValueError("Invalid fetch mode. Use 'none', 'one', 'all', or 'df'.")


def init_database(conn: "snowflake.connector.SnowflakeConnection") -> None:
    """Create database, schemas, and tables if they don't already exist."""
    bootstrap_path = pathlib.Path(__file__).with_name("bootstrap.sql")
    if not bootstrap_path.exists():
        raise FileNotFoundError(f"Bootstrap SQL not found at {bootstrap_path}")
    ddl = bootstrap_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in ddl.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)


def upsert_team(
    conn: "snowflake.connector.SnowflakeConnection",
    team_name: str,
    members: str,
    starting_budget: float,
) -> None:
    execute(
        conn,
        """
        INSERT INTO CORE.TEAMS (TEAM_NAME, MEMBERS, BUDGET)
        VALUES (%s, %s, %s)
        """,
        (team_name, members, starting_budget),
    )


def insert_car(
    conn: "snowflake.connector.SnowflakeConnection",
    team_id: int,
    car_name: str,
    top_speed_kmh: float,
    accel_0_100_s: float,
    handling: int,
    reliability: float,
    weight_kg: Optional[int] = None,
) -> None:
    execute(
        conn,
        """
        INSERT INTO CORE.CARS (
            TEAM_ID, CAR_NAME, TOP_SPEED_KMH, ACCELERATION_0_100_S,
            HANDLING, RELIABILITY, WEIGHT_KG
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (team_id, car_name, top_speed_kmh, accel_0_100_s, handling, reliability, weight_kg),
    )


def get_teams_df(conn: "snowflake.connector.SnowflakeConnection") -> pd.DataFrame:
    return execute(conn, "SELECT TEAM_ID, TEAM_NAME, MEMBERS, BUDGET FROM CORE.TEAMS ORDER BY TEAM_NAME", fetch="df")  # type: ignore


def get_cars_with_teams_df(conn: "snowflake.connector.SnowflakeConnection") -> pd.DataFrame:
    return execute(
        conn,
        """
        SELECT c.CAR_ID, c.CAR_NAME, c.TEAM_ID, t.TEAM_NAME,
            c.TOP_SPEED_KMH, c.ACCELERATION_0_100_S, c.HANDLING, c.RELIABILITY, c.WEIGHT_KG
        FROM CORE.CARS c
        JOIN CORE.TEAMS t ON t.TEAM_ID = c.TEAM_ID
        ORDER BY t.TEAM_NAME, c.CAR_NAME
        """,
        fetch="df",
    )


def record_transaction(
    conn: "snowflake.connector.SnowflakeConnection",
    team_id: int,
    race_id: Optional[int],
    amount: float,
    reason: str,
    currency: str = "USD",
) -> None:
    execute(
        conn,
        """
        INSERT INTO OPS.TRANSACTIONS (TEAM_ID, RACE_ID, AMOUNT, CURRENCY, REASON)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (team_id, race_id, amount, currency, reason),
    )
    execute(
        conn,
        "UPDATE CORE.TEAMS SET BUDGET = COALESCE(BUDGET, 0) + %s WHERE TEAM_ID = %s",
        (amount, team_id),
    )


def create_race(
    conn: "snowflake.connector.SnowflakeConnection",
    race_name: str,
    distance_km: float,
    entry_fee: float,
    prize_first: float,
    prize_second: float,
    prize_third: float,
) -> int:
    execute(
        conn,
        """
        INSERT INTO OPS.RACES (RACE_NAME, DISTANCE_KM, ENTRY_FEE, PRIZE_FIRST, PRIZE_SECOND, PRIZE_THIRD)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (race_name, distance_km, entry_fee, prize_first, prize_second, prize_third),
    )
    row = execute(conn, "SELECT MAX(RACE_ID) FROM OPS.RACES", fetch="one")
    return int(row[0]) if row and row[0] is not None else 0


def insert_race_result(
    conn: "snowflake.connector.SnowflakeConnection",
    race_id: int,
    car_id: int,
    team_id: int,
    finish_time_minutes: Optional[float],
    status: str,
    position: Optional[int],
) -> None:
    execute(
        conn,
        """
        INSERT INTO OPS.RACE_RESULTS (RACE_ID, CAR_ID, TEAM_ID, FINISH_TIME_MINUTES, STATUS, POSITION)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (race_id, car_id, team_id, finish_time_minutes, status, position),
    )
