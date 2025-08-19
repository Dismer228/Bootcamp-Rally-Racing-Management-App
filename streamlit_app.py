import random
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from snowflake_utils import (
    create_race,
    get_cars_with_teams_df,
    get_connection,
    get_teams_df,
    init_database,
    insert_car,
    record_transaction,
    upsert_team,
    insert_race_result,
)


# -----------------------------
# Page setup
# -----------------------------
st.set_page_config(page_title="Bootcamp Rally Manager", page_icon="🏁", layout="wide")
st.title("🏁 Bootcamp Rally Racing Management App")

st.caption(
    "Manage teams, cars, budgets, and run a 100 km rally simulation backed by Snowflake."
)

# -----------------------------
# Connection + bootstrap
# -----------------------------
@st.cache_resource(show_spinner=True)
def get_conn_cached():
    conn = get_connection()
    init_database(conn)
    return conn


conn = None
conn_error: Optional[str] = None
try:
    conn = get_conn_cached()
except Exception as e:
    conn_error = str(e)

if conn_error:
    st.error(
        "Cannot connect to Snowflake. Configure credentials via Streamlit secrets or env vars.\n"
        "See README for setup.\n\n" + conn_error
    )
    st.stop()

# -----------------------------
# Sidebar: Teams & Budgets
# -----------------------------
st.sidebar.header("Teams & Budgets")

if st.sidebar.button("🔄 Refresh teams"):
    st.sidebar.success("Refreshed")

teams_df = get_teams_df(conn)
st.sidebar.dataframe(teams_df, use_container_width=True, hide_index=True)

# Seed demo data (optional)
with st.sidebar.expander("Seed demo data"):
    if st.button("Add sample teams and cars"):
        try:
            upsert_team(conn, "Falcon Motorsport", "Alice,Bob", 10000)
            upsert_team(conn, "Thunder Racing", "Carol,Dan", 8000)
           
            teams = get_teams_df(conn)
            team_map = {r["TEAM_NAME"]: int(r["TEAM_ID"]) for _, r in teams.iterrows()}
           
            insert_car(conn, team_map["Falcon Motorsport"], "Falcon X1", 220.0, 5.2, 85, 0.92, 1200)
            insert_car(conn, team_map["Falcon Motorsport"], "Falcon X2", 210.0, 5.6, 80, 0.88, 1250)
            insert_car(conn, team_map["Thunder Racing"], "Storm ZR", 230.0, 4.9, 78, 0.85, 1180)
            st.success("Sample data added.")
        except Exception as e:
            st.error(f"Failed to seed data: {e}")

# -----------------------------
# Add Team (Bonus)
# -----------------------------
st.subheader("👥 Add Team (bonus)")
col1, col2, col3 = st.columns([2, 3, 2])
with col1:
    team_name = st.text_input("Team name")
with col2:
    team_members = st.text_input("Members (comma-separated)")
with col3:
    starting_budget = st.number_input("Starting budget (USD)", min_value=0.0, value=5000.0, step=500.0)

if st.button("➕ Add Team"):
    if not team_name.strip():
        st.warning("Team name is required.")
    else:
        try:
            upsert_team(conn, team_name.strip(), team_members.strip(), float(starting_budget))
            st.success(f"Team '{team_name}' added.")
        except Exception as e:
            st.error(f"Failed to add team: {e}")

# -----------------------------
# Add Car
# -----------------------------
st.subheader("🚗 Add Car")

teams_df = get_teams_df(conn)
team_options = [f"{row.TEAM_ID} - {row.TEAM_NAME}" for _, row in teams_df.iterrows()]
selected_team = st.selectbox("Assign to team", options=team_options if len(team_options) else ["No teams yet"], index=0 if len(team_options) else None)

col1, col2, col3 = st.columns(3)
with col1:
    car_name = st.text_input("Car name")
    accel = st.number_input("0-100 km/h (s)", min_value=2.0, max_value=15.0, value=5.5, step=0.1)
with col2:
    top_speed = st.number_input("Top speed (km/h)", min_value=120.0, max_value=400.0, value=220.0, step=1.0)
    handling = st.slider("Handling", min_value=50, max_value=100, value=80)
with col3:
    reliability = st.slider("Reliability", min_value=0.5, max_value=1.0, value=0.9, step=0.01)
    weight = st.number_input("Weight (kg)", min_value=800, max_value=2000, value=1200)

if st.button("➕ Add Car"):
    if not team_options:
        st.warning("Please add a team first.")
    elif not car_name.strip():
        st.warning("Car name is required.")
    else:
        try:
            team_id = int(selected_team.split(" - ")[0])
            insert_car(conn, team_id, car_name.strip(), float(top_speed), float(accel), int(handling), float(reliability), int(weight))
            st.success(f"Car '{car_name}' added to team {selected_team}.")
        except Exception as e:
            st.error(f"Failed to add car: {e}")

# -----------------------------
# Start Race!
# -----------------------------
st.subheader("🏎️ Start race!")

race_name = st.text_input("Race name", value=f"Rally {datetime.now().strftime('%Y-%m-%d %H:%M')}")
col1, col2, col3, col4 = st.columns(4)
with col1:
    distance_km = st.number_input("Distance (km)", min_value=10.0, max_value=1000.0, value=100.0, step=10.0)
with col2:
    entry_fee = st.number_input("Entry fee per team (USD)", min_value=0.0, max_value=100000.0, value=1000.0, step=100.0)
with col3:
    prize_first = st.number_input("1st prize (USD)", min_value=0.0, value=5000.0, step=500.0)
with col4:
    prize_second = st.number_input("2nd prize (USD)", min_value=0.0, value=3000.0, step=500.0)

col5, col6 = st.columns(2)
with col5:
    prize_third = st.number_input("3rd prize (USD)", min_value=0.0, value=1000.0, step=500.0)
with col6:
    track_preset = st.selectbox(
        "Track preset",
        ["Mixed (default)", "Fast asphalt", "Gravel twisty"],
    )


def simulate_time_minutes(
    top_speed_kmh: float,
    accel_0_100_s: float,
    handling: int,
    reliability: float,
    distance_km: float,
    preset: str,
) -> (float, bool):
    # Define track segments (length_km, speed_factor, handling_factor)
    if preset == "Fast asphalt":
        segments = [(distance_km, 1.05, 1.0)]
    elif preset == "Gravel twisty":
        segments = [(distance_km * 0.6, 0.8, 0.9), (distance_km * 0.4, 0.7, 0.85)]
    else: 
        segments = [
            (distance_km * 0.4, 0.95, 0.98),
            (distance_km * 0.3, 0.85, 0.92),
            (distance_km * 0.3, 0.75, 0.9),
        ]

    base_speed = float(top_speed_kmh)
    # Handling scale from 0.5 (50) to 1.0 (100)
    handling_scale = 0.5 + (max(50, min(100, handling)) - 50) / 100.0
    # Acceleration influences initial pace; faster accel yields slight boost
    accel_scale = max(0.9, min(1.05, 1.0 + (6.0 - accel_0_100_s) * 0.02))

    minutes = 0.0
    for length_km, speed_factor, handling_factor in segments:
        random_variation = random.uniform(0.92, 1.08)
        segment_speed = base_speed * handling_scale * accel_scale * speed_factor * handling_factor * random_variation
        segment_speed = max(60.0, segment_speed)
        hours = length_km / segment_speed
        minutes += hours * 60.0

    finish_probability = max(0.05, min(0.99, reliability))
    dnf = random.random() > finish_probability

    if not dnf:
        minutes *= random.uniform(0.98, 1.05)

    return minutes, dnf


if st.button("🏁 Start race!"):
    cars_df = get_cars_with_teams_df(conn)
    if cars_df.empty:
        st.warning("No cars available. Please add cars first.")
        st.stop()

    race_id = create_race(conn, race_name or "Rally", float(distance_km), float(entry_fee), float(prize_first), float(prize_second), float(prize_third))

    participating_team_ids = sorted(list({int(tid) for tid in cars_df["TEAM_ID"].tolist()}))
    for team_id in participating_team_ids:
        record_transaction(conn, team_id, race_id, -float(entry_fee), f"Entry fee for {race_name}")

    results: List[Dict] = []
    for _, row in cars_df.iterrows():
        mins, dnf = simulate_time_minutes(
            float(row["TOP_SPEED_KMH"]),
            float(row["ACCELERATION_0_100_S"]),
            int(row["HANDLING"]),
            float(row["RELIABILITY"]),
            float(distance_km),
            track_preset,
        )
        results.append(
            {
                "CAR_ID": int(row["CAR_ID"]),
                "CAR_NAME": row["CAR_NAME"],
                "TEAM_ID": int(row["TEAM_ID"]),
                "TEAM_NAME": row["TEAM_NAME"],
                "TIME_MIN": mins if not dnf else None,
                "DNF": dnf,
            }
        )

    if all(r["DNF"] for r in results):
        best_idx = min(range(len(results)), key=lambda i: results[i]["TIME_MIN"] or float("inf"))
        results[best_idx]["DNF"] = False
        if results[best_idx]["TIME_MIN"] is None:
            results[best_idx]["TIME_MIN"] = random.uniform(distance_km/3, distance_km/2)  # fallback


    finishers = [r for r in results if not r["DNF"]]
    finishers.sort(key=lambda r: r["TIME_MIN"]) 
    for pos, r in enumerate(finishers, start=1):
        r["POSITION"] = pos
    for r in results:
        if r.get("POSITION") is None:
            r["POSITION"] = None


    prizes = [float(prize_first), float(prize_second), float(prize_third)]
    for idx in range(min(3, len(finishers))):
        team_id = finishers[idx]["TEAM_ID"]
        amount = prizes[idx]
        if amount > 0:
            record_transaction(conn, team_id, race_id, amount, f"Prize for position {idx+1} in {race_name}")

    for r in results:
        insert_race_result(
            conn,
            race_id,
            int(r["CAR_ID"]),
            int(r["TEAM_ID"]),
            float(r["TIME_MIN"]) if r["TIME_MIN"] is not None else None,
            "FINISHED" if not r["DNF"] else "DNF",
            int(r["POSITION"]) if r["POSITION"] is not None else None,
        )

    results_df = pd.DataFrame(results)
    results_df_display = results_df.copy()
    if "TIME_MIN" in results_df_display.columns:
        results_df_display["TIME_MIN"] = results_df_display["TIME_MIN"].map(lambda x: round(x, 2) if isinstance(x, (float, int)) else None)

    st.success(f"Race '{race_name}' completed! Race ID: {race_id}")
    st.dataframe(
        results_df_display.sort_values(by=["DNF", "POSITION"], ascending=[True, True]),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("💰 Updated Budgets")
    st.dataframe(get_teams_df(conn), use_container_width=True, hide_index=True)
