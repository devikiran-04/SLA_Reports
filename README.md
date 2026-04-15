```markdown
# BEST SLA Monitoring Tool

A production-grade Python utility for generating **BEST (Bihar Electricity Supply & Transmission)** SLA compliance reports. Automates data extraction for DLP (Daily Load Profile) Not Received, BLP (Billing) Counts, and EOB (End of Bill) metrics.

---

## Features

| Feature | Description |
|---------|-------------|
| **Multi-Query Processing** | Executes 3 distinct SQL queries in sequence |
| **Day-wise DLP Analysis** | Iterates date range, writes column-by-column to manage large datasets |
| **Memory Optimized** | Uses XlsxWriter streaming + explicit garbage collection to handle millions of rows |
| **Modular SQL** | Separate queries for DLP, BLP, and EOB metrics |
| **PostgreSQL Backend** | SQLAlchemy engine with connection pooling |

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Python Script  │────▶│  SQLAlchemy     │────▶│  PostgreSQL      │
│  (this tool)     │     │  Engine        │     │  BEST_PRD DB      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               ▼
           ┌──────────────────────────────────────┐
           │  Excel Output (XlsxWriter)               │
           │  • DLP_Meter_List (column-wise per day)      │
           │  • BLP_NA (daily aggregation)               │
           │  • EOB_NA (end-of-bill records)               │
           └──────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites

```bash
# Python 3.9+
python --version

# PostgreSQL client libraries
pip install pandas sqlalchemy psycopg2-binary xlsxwriter

# Windows (for default path) or modify output_path variable
```

### Configuration

Edit these variables at the top of the script:

```python
# Database connection
DB_HOST =  # Your DB host
DB_NAME = # Database name
DB_USER = # DB username
DB_PASS = # DB password
DB_PORT = # PostgreSQL port

# Date range for DLP analysis (inclusive)
Start_date = datetime(2026, 3, 2)
End_date   = datetime(2026, 4, 1)

# Output file path
output_path = r"C:\Users\devikiran.p\Desktop\BEST SLA Python Script\BEST_APR_SLA.xlsx"
```

### Run

```bash
python BEST_SLA_Python.py
```

---

## How It Works

### Query 1: DLP Not Received (Daily Column-Wise)

For each day in the date range:
```
1. Query DISTINCT meter_numbers from fep.fep_csv_ed
2. Write results as a COLUMN in Excel (not rows)
3. This creates a sparse matrix view of missing DLP per day
```

**Why column-wise?**
- Prevents memory explosion with large date ranges
- XlsxWriter streams data to disk instead of holding in RAM
- Each day is processed independently with `gc.collect()`

### Query 2: BLP Counts (Daily Aggregation)

```sql
SELECT lp_date, COUNT(DISTINCT meter_number) AS meter_count
FROM dwh.communication_count_data
WHERE lp_date BETWEEN '20260302' AND '20260401'
  AND meter_number IN (SELECT meter_number FROM bkp.sat_final_meters)
  AND lp_cnt >= 96
GROUP BY lp_date
ORDER BY lp_date;
```

**Purpose:** Identify meters with >=96 communication records per day (healthy billing candidates)

### Query 3: EOB Received (Latest Record per Meter)

```sql
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
```

**Purpose:** Get the first (earliest) successful EOB record for each meter on April 1, 2026

---

## Output Format

| Sheet | Description | Structure |
|-------|-------------|-----------|
| `DLP_Meter_List` | Distinct meters with DLP not received | Date columns (2026-03-02 to 2026-04-01), Meter IDs as rows |
| `BLP_NA` | Daily billing communication counts | Two columns: `lp_date`, `meter_count` |
| `EOB_NA` | Latest EOB records per meter | Full record: `meter_number`, `meter_time`, `dcu_time` |

---

## Memory Management Strategy

| Technique | Implementation |
|-----------|---------------|
| **Streaming Writes** | XlsxWriter `write_column()` instead of pandas DataFrame |
| **Explicit Cleanup** | `del df_day, col_values` + `gc.collect()` |
| **Connection Pooling** | SQLAlchemy `pool_pre_ping=True` |
| **Date Chunking** | Process one day at a time, not entire range |

**Memory footprint:** ~50MB regardless of date range size (vs. GBs if using standard pandas)

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `MemoryError` | Reduce date range or increase system RAM |
| `Database connection failed` | Verify host/port/credentials; check VPN |
| `Permission denied on output path` | Ensure directory exists and is writable |
| `Empty DLP columns` | Verify `fep.fep_csv_ed` has data for date range |
| `BLP shows zero counts` | Check `dwh.communication_count_data` table population |
| `EOB shows no records` | Verify `fep.fep_csv_eob_ed` has April 1 data |

---

## Database Schema Reference

### Source Tables

| Table | Purpose |
|-------|---------|
| `fep.fep_csv_ed` | Failed/Error DLP records |
| `dwh.communication_count_data` | Communication event counts |
| `fep.fep_csv_eob_ed` | End-of-Bill successful records |
| `bkp.sat_final_meters` | Master meter list |


### Change Date Range

```python
# Modify these lines
Start_date = datetime(2026, 3, 2)  # Your start
End_date   = datetime(2026, 4, 1)  # Your end
```

---

## File Structure

```
BEST_SLA_Python/
├── BEST_SLA_Python.py    # Main script (this file)
├── requirements.txt      # pandas, sqlalchemy, psycopg2-binary, xlsxwriter
├── config.env             # DB credentials (optional)
└── output/
    └── BEST_APR_SLA.xlsx   # Generated report
```

---

## Requirements

```txt
pandas>=1.5.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
xlsxwriter>=3.1.0
```

---large DLP datasets efficiently.
