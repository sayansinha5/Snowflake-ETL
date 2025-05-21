import streamlit as st
import pandas as pd
import os
import logging
import time
from config import get_snowflake_session
from etl import run_pipeline

# Configure logging
logging.basicConfig(level=logging.INFO)

st.title("🚀 Snowflake ETL Pipeline App")

uploaded_file = st.file_uploader("Upload CSV file", type="csv")
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.dataframe(df)

    if st.button("Process File"):
        try:
            session = get_snowflake_session()
            uploaded_file.seek(0)

            # Save file with original name
            filename = uploaded_file.name
            with open(filename, "wb") as f:
                f.write(uploaded_file.getbuffer())

            logging.info(f"Local file saved as {filename}")

            # Upload to Snowflake stage
            stage_path = "@user_stage"
            logging.info(f"Uploading to stage: {stage_path}")

            # List stage contents before upload
            stage_files_before = session.sql(f"LIST {stage_path}").collect()
            logging.info(f"Stage contents before upload: {stage_files_before}")

            # Upload file
            session.file.put(filename, stage_path, overwrite=True)
            logging.info(f"File put command executed for {filename}")

            # Verify file exists in stage (check for both .csv and .csv.gz)
            stage_files_after = session.sql(f"LIST {stage_path}").collect()
            logging.info(f"Stage contents after upload: {stage_files_after}")

            # Check for both original filename and compressed version
            file_found = any(
                row['name'].endswith(filename) or
                row['name'].endswith(f"{filename}.gz")
                for row in stage_files_after
            )

            if file_found:
                st.success(f"Successfully uploaded {filename} to Snowflake stage!")
                # Clean up local file
                # os.remove(filename)
                # logging.info(f"Local file {filename} cleaned up")

                # Trigger Snowpipe
                st.info("Triggering Snowpipe...")
                session.sql("ALTER PIPE user_pipe REFRESH").collect()
                logging.info("Snowpipe refresh triggered")

                # Wait for Snowpipe to process (give it a few seconds)
                time.sleep(5)

                # Verify data was loaded
                raw_count = session.sql("SELECT COUNT(*) FROM users_raw").collect()[0][0]
                logging.info(f"Rows in users_raw after Snowpipe: {raw_count}")

                if raw_count > 0:
                    st.success(f"Data loaded into users_raw: {raw_count} rows")

                    # Run ETL Pipeline
                    st.info("Running ETL pipeline...")
                    if run_pipeline():
                        st.success("ETL pipeline completed successfully!")
                    else:
                        st.warning("ETL pipeline completed but no data was processed")
                else:
                    st.warning("No data was loaded into users_raw")
            else:
                st.error("File upload failed - file not found in stage")
                logging.error(f"File {filename} not found in stage after upload")

        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            logging.error(f"Processing error: {str(e)}", exc_info=True)

# Show output
st.header("Final Table: users_enriched")
try:
    session = get_snowflake_session()
    final_df = session.table("users_enriched").to_pandas()
    if not final_df.empty:
        st.dataframe(final_df)
    else:
        st.info("No data in users_enriched table yet")
except Exception as e:
    st.error(f"Error displaying results: {str(e)}")
    logging.error(f"Display error: {str(e)}", exc_info=True)
