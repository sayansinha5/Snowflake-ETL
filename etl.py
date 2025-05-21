import os
import logging
from typing import Optional
from datetime import datetime
from snowflake.snowpark.functions import col, lit, upper, count, current_timestamp
from snowflake.snowpark.exceptions import SnowparkSQLException
from config import get_snowflake_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_raw_table(session) -> bool:
    """Create the raw table if it doesn't exist."""
    try:
        # First check if table exists
        table_exists = session.sql("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = CURRENT_SCHEMA()
            AND table_name = 'USERS_RAW'
        """).collect()[0][0] > 0

        if not table_exists:
            logger.info("Creating users_raw table...")
            session.sql("""
                CREATE TABLE IF NOT EXISTS users_raw (
                    id INT,
                    name STRING,
                    email STRING,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """).collect()
            logger.info("users_raw table created successfully")

            # Create file format if it doesn't exist
            session.sql("""
                CREATE OR REPLACE FILE FORMAT my_csv_format
                TYPE = CSV
                FIELD_DELIMITER = ','
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                NULL_IF = ('NULL', 'null')
                EMPTY_FIELD_AS_NULL = TRUE
            """).collect()

            # Create stage if it doesn't exist
            session.sql("""
                CREATE OR REPLACE STAGE users_raw_stage
                FILE_FORMAT = my_csv_format
            """).collect()

            # Create Snowpipe
            session.sql("""
                CREATE OR REPLACE PIPE users_raw_pipe
                AUTO_INGEST = TRUE
                AS
                COPY INTO users_raw
                FROM @users_raw_stage
                FILE_FORMAT = my_csv_format
            """).collect()

            logger.info("Snowpipe created successfully")
        return True
    except SnowparkSQLException as e:
        logger.error(f"Failed to create raw table: {str(e)}")
        return False

def upload_to_stage(session, local_csv_path: str) -> bool:
    """Upload CSV file to Snowflake stage."""
    try:
        if not os.path.exists(local_csv_path):
            logger.error(f"CSV file not found: {local_csv_path}")
            return False

        # Get the pipe name for notification
        pipe_name = session.sql("""
            SELECT SYSTEM$PIPE_STATUS('users_raw_pipe')
        """).collect()[0][0]
        logger.info(f"Current pipe status: {pipe_name}")

        # Upload file to stage
        session.file.put(
            local_file_name=local_csv_path,
            stage_location="@users_raw_stage",
            auto_compress=False,
            overwrite=True
        )
        logger.info(f"Successfully uploaded {local_csv_path} to stage")

        # Verify file is in stage
        files = session.sql("LIST @users_raw_stage").collect()
        if not files:
            logger.error("No files found in stage after upload")
            return False

        logger.info(f"Files in stage: {[f.name for f in files]}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload file to stage: {str(e)}")
        return False

def copy_to_raw_table(session, file_name: str) -> bool:
    """Copy data from stage to raw table."""
    try:
        # Check if file exists in stage
        files = session.sql(f"LIST @users_raw_stage/{file_name}").collect()
        if not files:
            logger.error(f"File {file_name} not found in stage")
            return False

        # Copy data into raw table
        result = session.sql(f"""
            COPY INTO users_raw
            FROM @users_raw_stage/{file_name}
            FILE_FORMAT = my_csv_format
            VALIDATION_MODE = RETURN_ERRORS
        """).collect()

        # Check for errors
        if result and result[0].status == 'LOAD_FAILED':
            logger.error(f"Load failed: {result[0].first_error}")
            return False

        # Verify data was loaded
        row_count = session.sql("SELECT COUNT(*) FROM users_raw").collect()[0][0]
        if row_count == 0:
            logger.error("No data was loaded into users_raw")
            return False

        logger.info(f"Successfully loaded {row_count} rows into users_raw")
        return True
    except SnowparkSQLException as e:
        logger.error(f"Failed to copy data to raw table: {str(e)}")
        return False

def clean_data(session) -> bool:
    """Clean and transform the data."""
    try:
        # Verify raw table exists and has data
        table_exists = session.sql("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = CURRENT_SCHEMA()
            AND table_name = 'USERS_RAW'
        """).collect()[0][0] > 0

        if not table_exists:
            logger.error("users_raw table does not exist")
            return False

        raw_df = session.table("users_raw")

        # Check if raw table has data
        row_count = raw_df.select(count("*")).collect()[0][0]
        if row_count == 0:
            logger.warning("No data found in users_raw table")
            return False

        # Apply transformations with proper timestamp
        clean_df = raw_df.filter(col("name").is_not_null()) \
                        .with_column("email", upper(col("email"))) \
                        .with_column("processed_at", current_timestamp())

        # Save as final table
        clean_df.write.save_as_table("users_clean", mode="overwrite")
        logger.info(f"Successfully cleaned {row_count} records")
        return True
    except Exception as e:
        logger.error(f"Failed during data cleaning: {str(e)}")
        return False

def enrich_data(session) -> bool:
    """Enrich the cleaned data with additional information."""
    try:
        clean_df = session.table("users_clean")

        # Check if clean table has data
        row_count = clean_df.select(count("*")).collect()[0][0]
        if row_count == 0:
            logger.warning("No data found in users_clean table")
            return False

        # Add enrichment columns with proper timestamp
        enriched_df = clean_df.with_column("source", lit("uploaded_csv")) \
                             .with_column("enriched_at", current_timestamp())

        enriched_df.write.save_as_table("users_enriched", mode="overwrite")
        logger.info(f"Successfully enriched {row_count} records")
        return True
    except Exception as e:
        logger.error(f"Failed during data enrichment: {str(e)}")
        return False

def run_pipeline(local_csv_path: Optional[str] = None) -> bool:
    """Run the complete ETL pipeline."""
    try:
        session = get_snowflake_session()

        # Create raw table and Snowpipe
        if not create_raw_table(session):
            return False

        # If CSV path is provided, upload and copy data
        if local_csv_path:
            if not upload_to_stage(session, local_csv_path):
                return False

            file_name = os.path.basename(local_csv_path)
            if not copy_to_raw_table(session, file_name):
                return False

        # Run cleaning step
        if not clean_data(session):
            logger.error("Data cleaning step failed")
            return False

        # Run enrichment step
        if not enrich_data(session):
            logger.error("Data enrichment step failed")
            return False

        logger.info("ETL pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        return False
