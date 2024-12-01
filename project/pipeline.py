import pandas as pd
import sqlite3
from io import BytesIO
import requests
import logging

# Constants for wage calculations
HOURS_IN_YEAR = 2080
WEEKS_IN_YEAR = 52
MONTHS_IN_YEAR = 12

# Logging setup
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Utility Functions
def download_file(url: str) -> BytesIO:
    """Downloads a file from a URL and returns its content as a BytesIO object."""
    try:
        logger.debug(f"Downloading file from URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error occurred while downloading the file: {e}")
        raise


def parse_file(file_content: BytesIO, file_type: str, sheet_name: str = None) -> pd.DataFrame:
    """Parses a CSV or Excel file content into a pandas DataFrame."""
    try:
        logger.debug(f"Parsing file content. File type: {file_type}, Sheet name: {sheet_name}")
        if file_type == 'csv':
            return pd.read_csv(file_content)
        elif file_type == 'excel':
            data = pd.read_excel(file_content, sheet_name=None)
            if isinstance(data, dict):  # Handle multiple sheets
                if sheet_name is None:
                    sheet_name = list(data.keys())[0]
                    logger.debug(f"Defaulting to the first sheet: {sheet_name}")
                return data[sheet_name]
            return data
        else:
            raise ValueError("Invalid file type. Must be 'csv' or 'excel'.")
    except Exception as e:
        logger.error(f"Error parsing file: {e}")
        raise


# Data Transformation Functions
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes column names by converting to lowercase and replacing spaces with underscores."""
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df


def calculate_annual_wage(wage: float, unit: str) -> float:
    """Converts wage rates to annual values based on unit of pay."""
    unit = unit.lower() if pd.notnull(unit) else ''
    if unit == 'hour':
        return wage * HOURS_IN_YEAR
    if unit == 'week':
        return wage * WEEKS_IN_YEAR
    if unit == 'month':
        return wage * MONTHS_IN_YEAR
    return wage


def transform_h1b_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms and filters the H1B dataset for relevant roles."""
    logger.debug("Transforming H1B data.")
    df = standardize_column_names(df)
    df = df[df['employer_name'].str.contains("Microsoft", case=False, na=False)]
    df['received_date'] = pd.to_datetime(df['received_date'], errors='coerce')
    df = df[(df['received_date'] >= '2023-04-01') & (df['received_date'] <= '2023-06-30')]
    df['annual_wage'] = df.apply(lambda row: calculate_annual_wage(row['wage_rate_of_pay_from'], row['wage_unit_of_pay']), axis=1)
    return df


def transform_oews_data(df: pd.DataFrame, relevant_job_codes) -> pd.DataFrame:
    """Filters OEWS data for relevant job codes."""
    logger.debug("Transforming OEWS data.")
    df = standardize_column_names(df)
    df = df[df['soc_code'].isin(relevant_job_codes)]
    df['avg_local_wage'] = pd.to_numeric(df['annual_mean_wage'], errors='coerce')
    return df.dropna(subset=['avg_local_wage'])


def merge_datasets(h1b_df: pd.DataFrame, oews_df: pd.DataFrame) -> pd.DataFrame:
    """Merges H1B and OEWS datasets and calculates wage differences."""
    logger.debug("Merging H1B and OEWS datasets.")
    merged_df = h1b_df.merge(oews_df, left_on='job_code', right_on='soc_code', how='inner')
    merged_df['wage_diff'] = merged_df['annual_wage'] - merged_df['avg_local_wage']
    return merged_df


# Database Functions
def save_to_database(df: pd.DataFrame, db_name: str, table_name: str):
    """Saves a DataFrame to an SQLite database."""
    try:
        logger.debug(f"Saving data to database: {db_name}, table: {table_name}")
        with sqlite3.connect(db_name) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
    except Exception as e:
        logger.error(f"Error saving to database: {e}")
        raise


# Main Execution
if __name__ == "__main__":
    try:
        # URLs for datasets
        h1b_url = "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2023_Q3.xlsx"
        oews_url = "https://esd.wa.gov/media/2861"

        # Step 1: Fetch and parse datasets
        h1b_df = parse_file(download_file(h1b_url), file_type='excel')
        oews_df = parse_file(download_file(oews_url), file_type='excel', sheet_name='Statewide')

        # Step 2: Transform datasets
        h1b_transformed = transform_h1b_data(h1b_df)
        relevant_job_codes = h1b_transformed['job_code'].unique()
        oews_transformed = transform_oews_data(oews_df, relevant_job_codes)

        # Step 3: Merge datasets
        final_df = merge_datasets(h1b_transformed, oews_transformed)

        # Step 4: Save results to database
        db_name = "h1b_oews_analysis.db"
        save_to_database(h1b_transformed, db_name, "h1b_microsoft_roles")
        save_to_database(oews_transformed, db_name, "oews_microsoft_roles")
        save_to_database(final_df, db_name, "h1b_oews_combined")

        logger.info(f"Data pipeline complete. Database saved at {db_name}.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
