import os
from snowflake.snowpark.functions import col, upper
from config import get_snowflake_session

def run_etl(local_csv_path):
    session = get_snowflake_session()

    # 1. Create raw table
    session.sql("""
        CREATE OR REPLACE TABLE users_raw (
            id INT,
            name STRING,
            email STRING
        )
    """).collect()

    # 2. Upload CSV to table stage
    session.file.put(
        local_file_name=local_csv_path,
        stage_location="@%users_raw",
        auto_compress=False,
        overwrite=True
    )

    # 3. Copy data into raw table
    session.sql("""
        COPY INTO users_raw
        FROM @%users_raw/users.csv
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    """).collect()

    # 4. Transform using Snowpark
    df = session.table("users_raw")
    clean_df = df.filter(col("name").is_not_null()) \
                 .with_column("email", upper(col("email")))

    # 5. Save as final table
    clean_df.write.save_as_table("users_clean", mode="overwrite")

    return clean_df.to_pandas()
