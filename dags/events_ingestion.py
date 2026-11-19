import os
from datetime import datetime, timedelta, timezone

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task
from airflow.sdk.log import mask_secret

DB = "OPS_EVENTS"


@dag(schedule="@daily", start_date=datetime(2026, 11, 1), catchup=False, params={"lookback_days": 3})
def events_ingestion():

    @task
    def create_objects():
        bucket = os.environ["EVENTS_S3_BUCKET"]
        key_id = os.environ["AWS_ACCESS_KEY_ID"]
        secret = os.environ["AWS_SECRET_ACCESS_KEY"]
        mask_secret(key_id)
        mask_secret(secret)

        SnowflakeHook().run([
            f"CREATE DATABASE IF NOT EXISTS {DB}",
            f"CREATE SCHEMA IF NOT EXISTS {DB}.RAW",
            f"CREATE SCHEMA IF NOT EXISTS {DB}.CORE",
            f"CREATE FILE FORMAT IF NOT EXISTS {DB}.RAW.JSON_ARRAY TYPE = JSON STRIP_OUTER_ARRAY = TRUE",
            f"""CREATE STAGE IF NOT EXISTS {DB}.RAW.EVENTS_STAGE
                URL = 's3://{bucket}/raw/events/'
                CREDENTIALS = (AWS_KEY_ID = '{key_id}' AWS_SECRET_KEY = '{secret}')
                FILE_FORMAT = {DB}.RAW.JSON_ARRAY""",
            f"""CREATE TABLE IF NOT EXISTS {DB}.RAW.EVENTS_LANDING (
                RAW VARIANT, DT DATE, FILE_NAME VARCHAR, FILE_ROW NUMBER, LOADED_AT TIMESTAMP_LTZ)""",
            f"""CREATE TABLE IF NOT EXISTS {DB}.CORE.EVENTS (
                EVENT_ID VARCHAR NOT NULL PRIMARY KEY, EVENT_TYPE VARCHAR, EVENT_TS TIMESTAMP_TZ,
                CUSTOMER_ID VARCHAR, ORDER_ID VARCHAR, PAYLOAD VARIANT, DT DATE, FILE_NAME VARCHAR,
                INSERTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())""",
        ])

    @task
    def load_days(**context) -> list[str]:
        today = datetime.now(timezone.utc).date()
        lookback = int(context["params"]["lookback_days"])
        days = [str(today - timedelta(days=i)) for i in range(lookback + 1)]

        hook = SnowflakeHook()
        for day in days:
            hook.run(f"""
                COPY INTO {DB}.RAW.EVENTS_LANDING (RAW, DT, FILE_NAME, FILE_ROW, LOADED_AT)
                FROM (
                    SELECT $1,
                           TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)),
                           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
                    FROM @{DB}.RAW.EVENTS_STAGE/dt={day}/
                )
                ON_ERROR = SKIP_FILE
            """)
        return days
    
    @task
    def merge_events(days: list[str]):
        day_list = ", ".join(f"'{day}'" for day in days)
        SnowflakeHook().run(f"""
            MERGE INTO {DB}.CORE.EVENTS t
            USING (
                SELECT RAW:event_id::STRING AS EVENT_ID,
                       RAW:event_type::STRING AS EVENT_TYPE,
                       TRY_TO_TIMESTAMP_TZ(RAW:event_ts::STRING) AS EVENT_TS,
                       RAW:customer_id::STRING AS CUSTOMER_ID,
                       RAW:order_id::STRING AS ORDER_ID,
                       RAW:payload AS PAYLOAD,
                       DT,
                       FILE_NAME
                FROM {DB}.RAW.EVENTS_LANDING
                WHERE DT IN ({day_list}) AND RAW:event_id IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY LOADED_AT, FILE_NAME, FILE_ROW) = 1
            ) s
            ON t.EVENT_ID = s.EVENT_ID
            WHEN NOT MATCHED THEN
                INSERT (EVENT_ID, EVENT_TYPE, EVENT_TS, CUSTOMER_ID, ORDER_ID, PAYLOAD, DT, FILE_NAME)
                VALUES (s.EVENT_ID, s.EVENT_TYPE, s.EVENT_TS, s.CUSTOMER_ID, s.ORDER_ID, s.PAYLOAD, s.DT, s.FILE_NAME)
        """)

    @task
    def check_quality():
        hook = SnowflakeHook()

        duplicates = hook.get_first(f"""
            SELECT COUNT(*) FROM (
                SELECT EVENT_ID FROM {DB}.CORE.EVENTS GROUP BY EVENT_ID HAVING COUNT(*) > 1
            )
        """)[0]
        if duplicates:
            raise ValueError(f"{duplicates} duplicate event_ids in CORE.EVENTS")

        skipped_files = hook.get_records(f"""
            SELECT FILE_NAME, FIRST_ERROR_MESSAGE
            FROM TABLE({DB}.INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => '{DB}.RAW.EVENTS_LANDING',
                START_TIME => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
            ))
            WHERE STATUS <> 'Loaded'
        """)
        print(f"{len(skipped_files)} files skipped in the last 24h")
        for file_name, error in skipped_files:
            print(f"Skipped {file_name}: {error}")

    days = load_days()
    create_objects() >> days
    merge_events(days) >> check_quality()


events_ingestion()