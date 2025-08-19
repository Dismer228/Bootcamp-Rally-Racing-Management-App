## 🏁 Bootcamp Rally Racing Management App

Text-based Streamlit app connected to Snowflake to manage rally teams, cars, budgets, and simulate a 100 km rally.

### Features
- Add teams (bonus) and manage budgets
- Add cars and assign to teams
- Start race! Simulate 100 km rally with car characteristics and randomness
- Teams pay entry fee; winners receive prizes; budgets update
- Snowflake database, schemas, and tables are auto-created on first run

### Data model (Snowflake)
- Database: `BOOTCAMP_RALLY`
- Schemas: `CORE` (teams, cars), `OPS` (races, results, transactions)
- Tables: `CORE.TEAMS`, `CORE.CARS`, `OPS.RACES`, `OPS.RACE_RESULTS`, `OPS.TRANSACTIONS`

See `bootstrap.sql` for full DDL.

### Setup
1. Python 3.10+
2. Install deps:
```bash
pip install -r requirements.txt
```
3. Provide Snowflake credentials via either option:
   - Streamlit secrets (preferred): create `.streamlit/secrets.toml` with:
```toml
[snowflake]
account = "<your_account>"
user = "<your_user>"
password = "<your_password>"
role = "<role>"            # optional
warehouse = "<warehouse>"  # optional
database = "BOOTCAMP_RALLY" # optional
```
   - Environment variables:
```bash
export SNOWFLAKE_ACCOUNT="<your_account>"
export SNOWFLAKE_USER="<your_user>"
export SNOWFLAKE_PASSWORD="<your_password>"
export SNOWFLAKE_ROLE="<role>"            # optional
export SNOWFLAKE_WAREHOUSE="<warehouse>"  # optional
export SNOWFLAKE_DATABASE="BOOTCAMP_RALLY" # optional
```

### Run streamlit.io

Use streamlit_app.py as main file in streamlit.io

The app will create the database, schemas, and tables on first startup.

### Notes
- Entry fee is charged once per participating team per race.
- Prizes are awarded to the teams of the top 3 finishing cars.
- Budgets can go negative if teams lack funds.
