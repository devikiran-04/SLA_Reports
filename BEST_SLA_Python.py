import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import urllib.parse
import gc  # for explicit cleanup

# --- Database connection details ---
DB_HOST = "10.0.25.155"
DB_NAME = "best_prd"
DB_USER = "devuser"
DB_PASS = "devuser"
DB_PORT = "5432"

# --- Try connecting to PostgreSQL using SQLAlchemy ---
try:
    encoded_pass = urllib.parse.quote(DB_PASS)
    engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
        pool_pre_ping=True
    )
    with engine.connect() as con:
        con.execute(text("SELECT 1"))
    print("✅ Database connection successful")
except Exception as e:
    print("❌ Database connection failed:", e)
    raise SystemExit(1)

# --- Query 1: DLP Not Received (day-wise loop; write column-by-column) ---
print("▶ Executing DLP Not Received query (distinct meters per day → columns)...")
start_date = datetime(2026, 3, 2)
end_date   = datetime(2026, 4, 1)

sql_dlp = text("""
    SELECT DISTINCT meter_number
    FROM fep.fep_csv_ed
    WHERE meter_time = :meter_ts
      AND status = 'Success'
""")

# --- Query 2: BLP counts day-wise >=96 interval (UNCHANGED) ---
print("▶ Executing BLP Counts query...")
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

# --- Query 3: EOB received (for 2026-02-01) (UNCHANGED) ---
print("▶ Executing EOB Received query...")
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
# Use XlsxWriter for lower memory footprint and column-wise writing
output_path = r"C:\Users\devikiran.p\Desktop\BEST SLA Python Script\BEST_APR_SLA.xlsx"
print("▶ Saving results to Excel (XlsxWriter)...")

with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
    # 1) DLP sheet: write column-by-column to avoid huge in-memory DataFrame
    workbook  = writer.book
    ws_dlp    = workbook.add_worksheet("DLP_Meter_List")
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#FFF2CC", "border": 1})

    current_date = start_date
    col_idx = 0

    while current_date <= end_date:
        col_date_str = current_date.strftime("%Y-%m-%d")        # header text
        meter_ts     = current_date.strftime("%Y-%m-%d 00:00:00")

        print(f"   ➡ Writing DLP column for {col_date_str}")
        with engine.begin() as conn:
            df_day = pd.read_sql(sql_dlp, conn, params={"meter_ts": meter_ts})

        # Ensure string type to reduce memory surprises
        col_values = df_day["meter_number"].astype(str).tolist()

        # Write header
        ws_dlp.write(0, col_idx, col_date_str, header_fmt)
        # Write the column starting row 1 (below header)
        # write_column(row, col, data_list)
        ws_dlp.write_column(1, col_idx, col_values)

        # Explicit cleanup for safety on large loops
        del df_day, col_values
        gc.collect()

        current_date += timedelta(days=1)
        col_idx += 1

    # 2) BLP & 3) EOB sheets via normal pandas (these are much smaller)
    df_blp.to_excel(writer, sheet_name="BLP_NA", index=False)
    df_eob.to_excel(writer, sheet_name="EOB_NA", index=False)

# --- Dispose engine ---
engine.dispose()
print(f"✅ Report saved to {output_path} with three sheets")
