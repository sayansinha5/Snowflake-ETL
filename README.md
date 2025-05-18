# Snowflake ETL Streamlit App

This is a minimal ETL pipeline built with **Snowflake**, **Python (Snowpark)**, and **Streamlit**.
It allows you to upload a CSV file, load it into Snowflake, perform basic data cleaning using Snowpark, and view the results—all via a Streamlit UI.

## Features

- Upload a CSV file via the Streamlit interface
- Extract → Load → Transform using:
  - Snowflake for storage
  - Snowpark for transformation (Python API)
- Save clean data to a Snowflake table
- Display the transformed data in the browser

---

## Requirements

- Python 3.8+
- Snowflake account (with database/schema/warehouse)
- Install Python dependencies

## Quickstart Project
### Activate virtual environment
```
python -m venv venv
```

### For Linux systems
```
source venv/bin/activate
```

### For Windows systems
```
.\venv\Scripts\activate.bat
```

## Install dependencies
```
pip install -r requirements.txt
```

## Run the app
```
python -m streamlit run main.py
```

### For Linux systems
```
streamlit run main.py
```

### For Windows systems
```
streamlit run main.py
```
