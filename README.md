# S3 → Snowflake incremental event ingestion

Replaces the daily *full scan + overwrite* with an incremental load, orchestrated by Airflow.
Each run only reads the newest day prefixes in S3. Snowflake's COPY load history skips files that are
already loaded, and a MERGE on `event_id` keeps the final table free of duplicates.
Re-running any day is safe.

## Repo layout

```
generator/            mock data generator (Python, uv, Docker image)
dags/                 Airflow DAG: events_ingestion.py
docker-compose.yaml   Airflow 3.4 (standalone) + Postgres
.env.example          every setting the project needs (copy to .env)
```

## How it works

```
s3://<bucket>/raw/events/dt=YYYY-MM-DD/events_<uuid>.{json,data,txt}     (days in UTC)
        │  COPY INTO, one per day: today + lookback_days (default 3)
        ▼
OPS_EVENTS.RAW.EVENTS_LANDING   append-only: raw JSON + day, file name, row number, load time
        │  MERGE on event_id (insert-only), deduplicated with ROW_NUMBER()
        ▼
OPS_EVENTS.CORE.EVENTS          one row per event_id
```

DAG `events_ingestion` (daily):

1. `create_objects`: creates the database, schemas, file format, stage and tables with `IF NOT EXISTS`. It does nothing after the first run.
2. `load_days`: one `COPY INTO` per day prefix with `ON_ERROR = SKIP_FILE`.
3. `merge_events`: inserts events not yet in `CORE.EVENTS`, only for the days just loaded.
4. `check_quality`: fails on duplicate `event_id`s, and logs every file that failed to load in the last 24h.

**Generator:** writes 52 files per day for the last 3 days: 50 normal files, one existing file
re-delivered under a new name, and one truncated (corrupt) file. Each file holds 10–55 events and is
strictly between 2 KB and 16 KB. File names and extensions are random. Running it again adds new files.

## Edge cases

| Case | Handling |
|---|---|
| DAG re-run / task retry | COPY skips files it has already loaded; the MERGE inserts only new `event_id`s |
| Same events re-delivered in a new file | Landed twice, deduplicated by the MERGE (`QUALIFY ROW_NUMBER()`) |
| File overwritten under the same name | Its checksum changes, so COPY loads it again; the MERGE keeps the first version of each event |
| Corrupt / truncated file | `ON_ERROR = SKIP_FILE`: the file is skipped and the run continues; `check_quality` logs it |
| Late file in an older day prefix | Every run re-reads the last `lookback_days` days; only new files are loaded |
| Older late files / backfill | Run with a bigger window: `airflow dags test events_ingestion --conf '{"lookback_days": 10}'` |
| Unpredictable names / extensions | COPY reads every file under the day prefix; the format is set on the stage |
| Day without files | COPY processes 0 files, no error |
| Event without `event_id` / bad timestamp | Stays in landing and is not merged / `TRY_TO_TIMESTAMP_TZ` gives NULL instead of failing |

## Run it

### Prerequisites

- Docker with Compose, and about 4 GB of memory for Docker (Airflow runs in standalone mode).
- An S3 bucket, and an IAM user's access keys with:
  - `s3:PutObject` for the generator;
  - `s3:ListBucket`, `s3:GetBucketLocation`, `s3:GetObject` and `s3:GetObjectVersion` for the Snowflake stage.

  Use long-lived IAM user keys: the stage stores them, so temporary credentials would expire.
  (`AWS_SESSION_TOKEN` is only used by the generator.)
- A Snowflake account where you can use ACCOUNTADMIN (a free trial works).
- `openssl`, which comes with macOS and Linux. The AWS CLI is optional.

### 1. Clone and configure

```bash
git clone https://github.com/alifouzizadeh/s3-to-snowflake-events-ingestion.git
cd s3-to-snowflake-events-ingestion
cp .env.example .env
```

In `.env`, fill in `EVENTS_S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION`
(the bucket's region).

- Use no quotes, and put comments on their own line only.
- Leave `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` as it is: step 3 writes it.
- `.env` is git-ignored, so it stays local.

### 2. Generate the source data

```bash
docker run --rm --env-file .env <dockerhub-user>/ops-events-generator:latest
aws s3 ls s3://<bucket>/raw/events/ --recursive | wc -l    # optional: 156 on an empty bucket
```

To build the image yourself instead: `docker build -t ops-events-generator generator`.

### 3. Snowflake service user and Airflow connection

Create a key pair, kept outside the repo:

```bash
mkdir -p ~/.snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/airflow_rsa_key.p8 -nocrypt
openssl rsa -in ~/.snowflake/airflow_rsa_key.p8 -pubout -out ~/.snowflake/airflow_rsa_key.pub
grep -v "PUBLIC KEY" ~/.snowflake/airflow_rsa_key.pub | tr -d '\n'; echo    # public key for the SQL below
```

In a Snowsight worksheet, run this as ACCOUNTADMIN. If you don't use `COMPUTE_WH`, replace it here and in the next step.

```sql
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
CREATE USER IF NOT EXISTS AIRFLOW_LOADER
  TYPE = SERVICE
  DEFAULT_ROLE = SYSADMIN
  DEFAULT_WAREHOUSE = COMPUTE_WH
  RSA_PUBLIC_KEY = '<public key>';
GRANT ROLE SYSADMIN TO USER AIRFLOW_LOADER;
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();   -- your account identifier
```

Write the Airflow connection into `.env` (from the repo root):

```bash
ACCOUNT="<account identifier>"      # result of the last query, e.g. ABCDEFG-XY12345
WAREHOUSE="COMPUTE_WH"
KEY=$(base64 < ~/.snowflake/airflow_rsa_key.p8 | tr -d '\n')
grep -v '^AIRFLOW_CONN_SNOWFLAKE_DEFAULT=' .env > .env.tmp && mv .env.tmp .env
echo "AIRFLOW_CONN_SNOWFLAKE_DEFAULT={\"conn_type\":\"snowflake\",\"login\":\"AIRFLOW_LOADER\",\"extra\":{\"account\":\"$ACCOUNT\",\"warehouse\":\"$WAREHOUSE\",\"role\":\"SYSADMIN\",\"private_key_content\":\"$KEY\"}}" >> .env
```

### 4. Start Airflow and run the DAG

```bash
docker compose up -d
```

The first start pulls the images and takes a few minutes. Wait until http://localhost:8080 loads
(no login needed locally), then:

```bash
docker compose exec -e AIRFLOW__CORE__TEST_CONNECTION=Enabled airflow airflow connections test snowflake_default
docker compose exec airflow airflow dags test events_ingestion
```

Alternatively, unpause `events_ingestion` in the UI and trigger it.

### 5. Check the result

```sql
SELECT DT, COUNT(*) AS row_count, COUNT(DISTINCT EVENT_ID) AS ids
FROM OPS_EVENTS.CORE.EVENTS GROUP BY DT ORDER BY DT;   -- row_count = ids on every day
```

- The `check_quality` log lists the 3 corrupt files, one per day.
- Run the DAG again: no new rows appear in `RAW.EVENTS_LANDING` or `CORE.EVENTS`.

### Troubleshooting

- **Port 8080 already in use:** change `ports` in `docker-compose.yaml`.
- **`Connection not found` right after starting:** wait a few seconds and run the command again.
- **Airflow is slow or restarts:** give Docker more memory (with Colima: `colima start --cpu 4 --memory 6`).
- **Changed the AWS keys or bucket after the first run:** the stage keeps the old values. Run `DROP STAGE OPS_EVENTS.RAW.EVENTS_STAGE;` and run the DAG again; `create_objects` recreates it.

### Clean up

```bash
docker compose down
```

```sql
DROP DATABASE OPS_EVENTS;
DROP USER AIRFLOW_LOADER;
```

Then delete the IAM access key.

## Trade-offs

- **Incremental instead of full scan + overwrite.** Cost scales with new files, not with history, and there is no moment when the target table is empty or half-written.
- **Two dedup layers.** COPY load history (kept 64 days) stops files loading twice. The MERGE stops events landing twice: from re-delivered files, from a load more than 64 days later, or after the landing table is recreated, which resets its load history. Keep `lookback_days` under 64.
- **Failed files are retried every run** while they are inside the lookback window. That's negligible here; in production I'd move them to a quarantine prefix and alert.
- **Object creation lives in the DAG**, so the project runs from scratch in one step. In production it belongs in Terraform or migrations, and the loader role would only get the grants it needs to load and merge.
- **Stage uses access keys** for a quick local setup. In production I'd use a storage integration (IAM role, no keys); that's a one-line change in `create_objects`.
- **The service user has SYSADMIN** for simplicity. In production it would get a dedicated role with only the grants it needs.
- **The MERGE matches against all of `CORE.EVENTS`**, so it catches re-deliveries across days. That's fine at this size; at scale I'd cluster by `DT` and limit the match window.
- **The landing table grows forever.** It needs a retention policy once the data is merged.
- **Daily schedule.** For lower latency, run it hourly or switch to Snowpipe auto-ingest.
- **My test setup** used a bucket in eu-north-1 and Snowflake in eu-central-1. Cross-region transfer is negligible here; in production both would be in the same region.

## With more time

- Terraform for the bucket, the IAM policy and the Snowflake objects, roles and grants.
- A storage integration and least-privilege roles; secrets from AWS Secrets Manager via an Airflow secrets backend instead of `.env`.
- A quarantine prefix for bad files, with alerts when `check_quality` fails.
- Freshness and volume checks per day (expected vs. loaded events).
- Tests in CI: a generator unit test (pytest + moto), a DAG import test, and ruff.
- Schema validation on the event payload, and dbt models on top of `CORE.EVENTS`.
