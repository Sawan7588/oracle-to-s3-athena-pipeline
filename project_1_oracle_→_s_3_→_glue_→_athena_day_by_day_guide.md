# Project 1 — Oracle → S3 → Glue → Athena (Day‑by‑Day Guide)

**Region:** ap-south-1 (Asia Pacific • Mumbai)\
**Goal:** Build a production‑style *batch* pipeline that ingests CSV files (exported from Oracle), lands them on S3 (Bronze), cleans/types them with Glue into Parquet (Silver), applies business transforms/aggregations (Gold), and queries with Athena.\
**You completed Days 1–5 — this doc captures every step so you can paste it into GitHub as **``** and revise anytime.**

---

## Repository Layout (suggested)

```
project1-oracle-s3-glue-athena/
├─ data/
│  └─ customer_policy_transactions_sample.csv        # ~100 rows sample
├─ glue_scripts/
│  ├─ bronze_to_silver_customer_transactions.py     # copy from Glue console Script tab
│  └─ silver_to_gold_transformations.py             # copy from Glue console Script tab
├─ diagrams/
│  └─ architecture.png                              # your flow diagram
├─ .github/workflows/
│  └─ placeholder.txt                               # (optional) future CI
└─ README.md
```

> **Note:** To version your Glue scripts, open each Glue job → **Script** tab → **Select All → Copy** → paste into the files above.

---

## S3 Data Lake Layout

```
s3://<your-bucket>/
  ├─ bronze/
  │   └─ customer_policy_transactions/              # raw CSV drops
  ├─ silver/
  │   └─ customer_policy_transactions/              # Parquet, partitioned by year/month/day
  └─ gold/
      ├─ policy_transactions_curated/               # curated, partitioned by year/month/day
      └─ daily_product_metrics/                     # KPI aggregates, partitioned by year/month/day
```

---

# Day 1 — Repo, Sample Data, S3 Buckets

### 1) Create GitHub repo & local folders

```bash
mkdir -p project1-oracle-s3-glue-athena/data
cd project1-oracle-s3-glue-athena
```

### 2) Create sample CSV (\~100 rows)

Create `data/customer_policy_transactions_sample.csv` with columns:

```
Policy_ID,Customer_ID,Product_Type,Premium_Amount,Payment_Status,Transaction_Date,Claim_Amount,Claim_Status,Agent_ID,Last_Updated,Region,Source_System
```

- Mimic realistic insurance data across multiple days, products, and statuses.
- Keep dates consistent (e.g., 2025‑07‑01 .. 2025‑07‑10) so partitions are meaningful later.

### 3) Commit & push

```bash
git init
git add .
git commit -m "Day1: repo scaffold + sample CSV"
git branch -M main
git remote add origin https://github.com/<you>/<repo>.git
git push -u origin main
```

### 4) Create S3 bucket & folders (Console)

- S3 → **Create bucket**: `de-project1-<unique>` (Region: ap-south-1).
- Inside the bucket, create prefixes:
  - `bronze/customer_policy_transactions/`
  - `silver/customer_policy_transactions/`
  - `gold/policy_transactions_curated/`
  - `gold/daily_product_metrics/`

### 5) Upload CSV to Bronze

- Upload `data/customer_policy_transactions_sample.csv` to:\
  `s3://de-project1-<unique>/bronze/customer_policy_transactions/`

**Acceptance (Day 1)**

- GitHub repo exists with the CSV.
- S3 bucket exists with **bronze/silver/gold** prefixes.
- CSV is in **bronze/**.

---

# Day 2 — Glue Database, Bronze Crawler, Athena Validation (Console)

### 1) Create Glue Database

- Glue → **Databases** → **Add database**: `insurance_data_db`

### 2) Create Bronze Crawler

- Glue → **Crawlers** → **Create crawler**
  - Name: `project1-bronze-crawler`
  - Data source: S3 path `s3://de-project1-<unique>/bronze/customer_policy_transactions/`
  - IAM role: `AWSGlueServiceRole-Project1` (S3 read + Glue permissions)
  - Target database: `insurance_data_db`
  - Table prefix (optional): `bronze_`
  - **Run** the crawler.

### 3) Verify table

- Glue → **Tables**: expect `bronze_customer_policy_transactions` with correct schema.

### 4) Query in Athena

- Athena → set **Query result location** (one-time) → choose `s3://de-project1-<unique>/athena-results/`
- Database: `insurance_data_db`
- Run:

```sql
SELECT COUNT(*) FROM insurance_data_db.bronze_customer_policy_transactions;
SELECT * FROM insurance_data_db.bronze_customer_policy_transactions LIMIT 10;
```

**Acceptance (Day 2)**

- Glue DB + Bronze table visible.
- Athena returns row count and preview.

---

# Day 3 — Glue ETL: Bronze → Silver (Parquet + Partitions)

### 1) Create Glue Job (Spark, Python)

- Name: `bronze_to_silver_customer_transactions`
- Glue version: **4.0** (Spark 3.3)
- IAM role: `AWSGlueServiceRole-Project1`
- Default arguments (examples):
  - `--SOURCE_S3_PATH` = `s3://de-project1-<unique>/bronze/customer_policy_transactions/`
  - `--TARGET_S3_PATH` = `s3://de-project1-<unique>/silver/customer_policy_transactions/`
  - `--enable-metrics` = `true`

> **Script summary:**
>
> - Read CSV from Bronze (trim, type-cast numeric/date/timestamp).
> - Normalize `Product_Type` (e.g., "MutualFund" → "Mutual Fund").
> - Deduplicate by `(Policy_ID, Transaction_Date)` using `Last_Updated`.
> - Add partitions: `year/month/day` from `Transaction_Date`.
> - Write **Parquet** partitioned to Silver.

> **How to save script to repo later:** Open job → **Script** tab → copy all → paste into `glue_scripts/bronze_to_silver_customer_transactions.py` in your repo.

### 2) Run job & verify

- S3 → confirm Parquet files appear under `silver/customer_policy_transactions/year=.../month=.../day=.../`

### 3) Create Silver Crawler

- Name: `project1-silver-crawler`
- Path: `s3://de-project1-<unique>/silver/customer_policy_transactions/`
- DB: `insurance_data_db`
- Prefix: `silver_`
- **Run**

### 4) Athena queries

```sql
SELECT COUNT(*) FROM insurance_data_db.silver_customer_policy_transactions;
SELECT DISTINCT Product_Type FROM insurance_data_db.silver_customer_policy_transactions;
SELECT year, month, day, COUNT(*) cnt
FROM insurance_data_db.silver_customer_policy_transactions
GROUP BY 1,2,3 ORDER BY 1,2,3;
```

**Acceptance (Day 3)**

- Glue job succeeds.
- Silver Parquet exists with partitions.
- Silver table crawled.
- Athena verifies types/rows.

---

# Day 4 — Silver → Gold (Business Transforms + KPIs)

### 1) Create Glue Job (Silver → Gold)

- Name: `silver_to_gold_transformations`
- Args:
  - `--SILVER_S3_PATH` = `s3://de-project1-<unique>/silver/customer_policy_transactions/`
  - `--GOLD_CURATED_PATH` = `s3://de-project1-<unique>/gold/policy_transactions_curated/`
  - `--GOLD_METRICS_PATH` = `s3://de-project1-<unique>/gold/daily_product_metrics/`

> **Script summary:**
>
> - Read Silver (Parquet).
> - Filters: positive amounts, not-null keys.
> - Derived: `Has_Claim`, `Payment_Flag`, `Premium_to_Claim_Ratio`.
> - **Gold 1 (Curated):** select clean columns, keep `year/month/day`, write Parquet partitioned.
> - **Gold 2 (Metrics):** group by day & product; output policies, transactions, totals, averages, rates.

### 2) Crawl Gold

- Crawler: `project1-gold-crawler` → path `s3://de-project1-<unique>/gold/` → DB `insurance_data_db` → prefix `gold_` → **Run**.

### 3) Athena verification

```sql
SELECT COUNT(*) FROM insurance_data_db.gold_policy_transactions_curated;
SELECT * FROM insurance_data_db.gold_daily_product_metrics ORDER BY year,month,day,Product_Type LIMIT 20;
```

**Acceptance (Day 4)**

- Gold Parquet created for **curated** and **metrics**.
- Two Gold tables appear in Glue Catalog.
- Athena queries return expected values.

### (Optional Enhancements you explored)

- **Append + Job Bookmarks** to make Bronze→Silver incremental.
- **Dynamic partition overwrite** for metrics; append with dedupe for curated.
- **Guardrail**: WARN if `avg_premium` is 0/NULL.

---

# Day 5 — Automation, Version Control, Documentation

## A) Glue Workflow (Console)

**Workflow name:** `oracle_to_athena_workflow`

1. **Add Trigger** (On‑demand or Scheduled) → **Action:** Job `bronze_to_silver_customer_transactions`  → save as `start_bronze_to_silver`.
2. **Add Trigger** (Conditional) → **Condition:** Job `bronze_to_silver_customer_transactions` **SUCCEEDED** → **Action:** Job `silver_to_gold_transformations` → save as `after_bronze_success`.
3. *(Optional)* **Add Trigger** (Conditional) → **Condition:** Job `silver_to_gold_transformations` **SUCCEEDED** → **Action:** Crawler `project1-gold-crawler` → save as `after_gold_job_success`.
4. **Validate → Save → Run**. Watch the **Runs** tab.

## B) Version control the Glue scripts

1. Open each job → **Script** tab → **Select All → Copy**.
2. Paste into:
   - `glue_scripts/bronze_to_silver_customer_transactions.py`
   - `glue_scripts/silver_to_gold_transformations.py`
3. Commit & push:

```bash
git add glue_scripts/
git commit -m "Add Glue ETL scripts"
git push
```

## C) Document the project (README)

Include sections:

- **Overview** (business problem & goal).
- **Architecture** (add `diagrams/architecture.png`).
- **Data model** (columns, types, partitioning).
- **How to run** (manual & via Workflow).
- **Athena sample queries**.
- **Operational notes** (bookmarks, partitions, guardrails).
- **Costs & cleanup** (delete dev resources).

---

## IAM Role Notes (quick)

Role used by Glue jobs (e.g., `AWSGlueServiceRole-Project1`) should allow:

- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on your bucket paths.
- Glue service permissions (managed policy `AWSGlueServiceRole`).
- CloudWatch Logs write.

---

## Sample Athena Queries (handy)

```sql
-- Latest 10 curated transactions
SELECT Policy_ID, Product_Type, Premium_Amount, Payment_Status, Transaction_Date
FROM insurance_data_db.gold_policy_transactions_curated
ORDER BY Transaction_Date DESC
LIMIT 10;

-- Daily KPIs by product
SELECT year, month, day, Product_Type, transactions, total_premium, total_claim,
       ROUND(paid_rate,3) AS paid_rate, ROUND(claim_rate,3) AS claim_rate
FROM insurance_data_db.gold_daily_product_metrics
ORDER BY year, month, day, Product_Type
LIMIT 100;

-- Partition pruning example
SELECT COUNT(*)
FROM insurance_data_db.gold_policy_transactions_curated
WHERE year=2025 AND month='07';
```

---

## Troubleshooting Cheatsheet

- **Crawler created wrong schema:** delete table, fix CSV headers/quotes, re‑crawl.
- **Job can’t read/write S3:** check IAM role & exact S3 path; bucket region must be ap-south-1.
- **Workflow doesn’t start:** ensure start trigger is **On‑demand/Scheduled** and has the **Job action** attached.
- **Conditional trigger not firing:** verify Job state = **SUCCEEDED** for the correct job.

---

## Cleanup

- Stop/disable Workflow schedule if any.
- Delete S3 objects (or bucket) after testing.
- Delete Glue jobs/crawlers/workflow to avoid costs.

---

### Done ✅

You now have a complete, interview‑ready AWS batch pipeline with Bronze/Silver/Gold, Athena queries, and Workflow orchestration — captured as a single guide to drop into your G
