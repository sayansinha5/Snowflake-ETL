-- 1. Stage
CREATE OR REPLACE STAGE user_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for user data uploads';

-- 2. Table
CREATE OR REPLACE TABLE users_raw (
    id INT,
    name STRING,
    email STRING
);

-- Create enriched table
CREATE OR REPLACE TABLE users_enriched (
    id INT,
    name STRING,
    email STRING,
    source STRING
);

-- 3. File Format
CREATE OR REPLACE FILE FORMAT user_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

-- 4. Pipe
CREATE OR REPLACE PIPE user_pipe AS
COPY INTO users_raw
FROM @user_stage
FILE_FORMAT = user_csv_format;

-- Optional: Resume Snowpipe if required
-- ALTER PIPE user_pipe REFRESH;
