
# Databricks Cloud Lakehouse & ETL Pipeline — Dashboard Royale

This repository contains the scalable data engineering pipeline and lakehouse architecture powering our **TAMU Datathon Dashboard Royale** project ([The Gilded Ace — Decision Room](https://github.com/NikhilMuvva/gilded-ace-decision-room)).

Built using **Databricks Serverless Compute**, **PySpark**, and **Delta Lake**, this pipeline transforms raw casino tracking records into high-performance, enriched analytical views for frontend dashboard consumption.

---

## 🏗️ Architecture & Databricks Value-Add

In casino analytics, raw hourly operational meters and player logs scale to millions of records (`machine_hour` has 1.3M+ rows; daily activity contains 56k+ entries). Performing on-the-fly aggregations and complex joins directly in a web browser or lightweight Node/Vite frontend causes UI freezing, memory bloat, and request timeouts.

### How Databricks Improves the Solution:
1. **Compute Offloading:** Heavy joining and window aggregations are executed on Databricks Serverless clusters, reducing 56,110 daily records to 310 clean, pre-computed machine summaries.
2. **Casino Math & Edge-Case Integrity:**
   - **Hold Ratio of Sums:** Adheres to regulatory standards by computing hold as `SUM(house_win) / SUM(coin_in)` rather than averaging daily ratios.
   - **Calendar-Aware Availability:** Joins `calendar_day` on `gaming_date` to divide `available_hours` by `scheduled_seconds / 3600.0`, accurately adjusting for non-24-hour days (such as the 23-hour DST transition on March 8, 2025).
   - **Lifecycle Filtering:** Filters out inactive/pre-install/post-retirement machine-days (`active == True`), eliminating 1,018 ghost records that distort floor averages.
3. **Delta Lake & Zero-Denominator Safety:** Implements PySpark `when().otherwise()` conditionals to prevent division-by-zero exceptions and commits output to a managed Delta table (`default.machine_metrics_clean`) with transactional reliability (`_delta_log`).
4. **Live Query Capability:** The Delta table is exposed via a **Databricks Serverless SQL Warehouse** for live SQL queries (`@databricks/sql` connector).

---

## 📂 Repository Contents

| File | Description |
| :--- | :--- |
| **`Casino Machine Metrics ETL.ipynb`** | The complete PySpark ETL notebook executed in Databricks, including data ingestion, join logic, metric aggregation, Delta table persistence, and output validation. |
| **`machine_metrics_clean.csv`** | Exported 310-row dataset generated from the clean Delta table, providing pre-aggregated metrics (`total_house_win`, `expected_win`, `adjusted_hold`, `availability_ratio`). |
| **`databricks_connection_info.md`** | Connection credentials, hostname, HTTP path, and SQL warehouse parameters for direct dashboard connectivity. |

---

## 🚀 Pipeline Execution Steps

1. **Ingestion:** Reads `prepared_daily_view.parquet` and `calendar_day.parquet` from the Databricks Lakehouse storage environment.
2. **Transformation:**
   ```python
   # Excerpt from Casino Machine Metrics ETL.ipynb
   active_machine_days = prepared_daily_view.join(
       calendar_day.select("gaming_date", "scheduled_seconds"),
       on="gaming_date",
       how="inner"
   ).filter(F.col("active") == True)
