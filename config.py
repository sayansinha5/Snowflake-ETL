import os
import logging
from typing import Dict, Any
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_env_vars() -> bool:
    """Validate that all required environment variables are set."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA"
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    return True

def get_connection_parameters() -> Dict[str, Any]:
    """Get Snowflake connection parameters from environment variables."""
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }

def get_snowflake_session() -> Session:
    """Create and return a Snowflake session."""
    try:
        # Load environment variables
        load_dotenv()

        # Validate environment variables
        if not validate_env_vars():
            raise ValueError("Missing required environment variables")

        # Get connection parameters
        connection_parameters = get_connection_parameters()

        # Create and return session
        session = Session.builder.configs(connection_parameters).create()

        # Test connection
        session.sql("SELECT CURRENT_TIMESTAMP()").collect()
        logger.info("Successfully connected to Snowflake")

        return session
    except SnowparkSQLException as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while connecting to Snowflake: {str(e)}")
        raise
