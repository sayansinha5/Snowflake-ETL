import schedule
import time
import logging
from datetime import datetime
from etl import run_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

def run_scheduled_pipeline():
    try:
        logging.info("Starting ETL pipeline run...")
        run_pipeline()
        logging.info("ETL pipeline completed successfully")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}", exc_info=True)

# Schedule the pipeline to run every minute
schedule.every(1).minutes.do(run_scheduled_pipeline)

logging.info("ETL Scheduler started...")
while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        logging.error(f"Scheduler error: {str(e)}", exc_info=True)
        time.sleep(60)  # Wait a minute before retrying if there's an error
