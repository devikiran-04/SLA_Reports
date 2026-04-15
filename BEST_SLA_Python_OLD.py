import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib.parse
import os

# --- Database connection details ---
DB_HOST = 
DB_NAME = 
DB_USER = 
DB_PASS = 
DB_PORT = 

# --- Try connecting to PostgreSQL ---
try:
    encoded_pass = urllib.parse.quote(DB_PASS)
    engine = create_engine(f'postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    print("Database connection successful")
except Exception as e:
    print("Database connection failed:", e)
    exit(1)

# --- Query 1: DLP Not Received ---
print("Executing DLP Not Received query...")
start_date = datetime(2026, 3, 2)
end_date   = datetime(2026, 4, 1)

results = []
current_date = start_date
while current_date <= end_date:
    day_str = current_date.strftime("%Y%m%d")          # YYYYMMDD format
    day_ts  = current_date.strftime("%Y-%m-%d 00:00:00")

    query_dlp = f"""
        SELECT '{day_str}' AS run_date, meter_number
        FROM bkp.sat_final_meters
        EXCEPT
        SELECT '{day_str}' AS run_date, meter_number
        FROM (
            SELECT meter_number, meter_time, dcu_time,
                   ROW_NUMBER() OVER (PARTITION BY meter_number ORDER BY dcu_time) rn
            FROM fep.fep_csv_ed
            WHERE meter_time = '{day_ts}'
              AND status = 'Success'
              AND meter_number IN (SELECT meter_number FROM bkp.sat_final_meters)
        ) k
        WHERE rn = 1;
    """

    print(f"   ➡ Running DLP for {day_str}")
    df_day = pd.read_sql(query_dlp, engine)
    results.append(df_day)
    current_date += timedelta(days=1)

df_dlp = pd.concat(results, ignore_index=True)

# --- Query 2: BLP counts ---
print("Executing BLP Counts query...")
query_blp = """
SELECT lp_date, COUNT(DISTINCT meter_number) AS meter_count
FROM dwh.communication_count_data
WHERE lp_date BETWEEN '20260302' AND '20260401'
  AND meter_number IN (SELECT meter_number FROM bkp.sat_final_meters)
  AND lp_cnt >= 96
GROUP BY lp_date
ORDER BY lp_date;
"""
df_blp = pd.read_sql(query_blp, engine)

# --- Query 3: EOB received (for 2026-03-01) ---
print("Executing EOB Received query...")
query_eob = """
SELECT *
FROM (
    SELECT meter_number, meter_time, dcu_time,
           ROW_NUMBER() OVER (PARTITION BY meter_number ORDER BY dcu_time) rn
    FROM fep.fep_csv_eob_ed
    WHERE meter_time = '2026-04-01 00:00:00'
      AND status = 'Success'
      AND meter_number IN (SELECT meter_number FROM bkp.sat_final_meters)
) k
WHERE rn = 1;
"""
df_eob = pd.read_sql(query_eob, engine)

# --- Save all results into one Excel file ---
output_path = r"C:\Users\devikiran.p\Desktop\BEST SLA Python Script\BEST_APRIL_SLA.xlsx"

# Create directory if it doesn't exist
os.makedirs(os.path.dirname(output_path), exist_ok=True)

print(f"Saving results to {output_path}...")
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_dlp.to_excel(writer, sheet_name="DLP_NA", index=False)
    df_blp.to_excel(writer, sheet_name="BLP_NA", index=False)
    df_eob.to_excel(writer, sheet_name="EOB_SLA", index=False)

# --- Dispose engine ---
engine.dispose()
print(f"✅ Report saved to {output_path} with three sheets")
