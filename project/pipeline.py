import os
import pandas as pd
import sqlite3
from io import BytesIO
import requests
import matplotlib.pyplot as plt
import seaborn as sns

# Debug variable 
DEBUG = True

def debug_print(message):
    """Prints a debug message if DEBUG is True."""
    if DEBUG:
        print(f"[DEBUG] {message}")

def download_file(url: str) -> BytesIO:
    """Downloads a file from a URL and returns its content as a BytesIO object."""
    try:
        debug_print(f"Downloading file from URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        return BytesIO(response.content)
    except Exception as e:
        print(f"Error occurred while downloading the file: {str(e)}")
        raise

def parse_file(file_content: BytesIO, file_type: str = None, sheet_name: str = None) -> pd.DataFrame:
    """Parses a CSV or Excel file content into a pandas DataFrame."""
    try:
        debug_print(f"Parsing file content. File type: {file_type}, Sheet name: {sheet_name}")

        if file_type == 'csv':
            df = pd.read_csv(file_content)
        elif file_type == 'excel':
            data = pd.read_excel(file_content, sheet_name=None)  # Read all sheets
            if isinstance(data, dict):  # Handle multiple sheets
                debug_print(f"Excel file contains multiple sheets: {list(data.keys())}")
                if sheet_name is None:
                    first_sheet_name = list(data.keys())[0]
                    debug_print(f"Defaulting to the first sheet: {first_sheet_name}")
                    df = data[first_sheet_name]
                else:
                    if sheet_name not in data:
                        raise ValueError(f"Sheet name '{sheet_name}' not found in the Excel file.")
                    df = data[sheet_name]
            else:
                df = data
        else:
            raise ValueError("file_type must be either 'csv' or 'excel'")

        debug_print(f"Columns in the parsed dataset: {list(df.columns)}")
        return df

    except Exception as e:
        print(f"Error occurred while parsing the file: {str(e)}")
        raise

def transform_h1b_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms and filters the H1B dataset for all companies."""
    debug_print("Starting H1B data transformation.")
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    debug_print(f"Initial H1B dataset rows: {len(df)}")

    # Filter for CASE_STATUS == "CERTIFIED"
    df = df[df['case_status'].str.strip().str.upper() == 'CERTIFIED']
    debug_print(f"Rows after filtering for certified cases: {len(df)}")

    # Filter for dates within a specific range
    df['received_date'] = pd.to_datetime(df['received_date'], errors='coerce')
    df = df[(df['received_date'] >= '2023-04-01') & (df['received_date'] <= '2023-06-30')]
    debug_print(f"Rows after filtering by date range: {len(df)}")

    # Standardize SOC codes and remove decimals
    df = df.rename(columns={'soc_code': 'job_code', 'soc_title': 'occupation_title'})
    df['job_code'] = df['job_code'].astype(str).str.split('.').str[0].str.strip()

    # Drop rows with null job codes
    df = df.dropna(subset=['job_code'])
    debug_print(f"Rows after removing null job codes: {len(df)}")

    # Standardize wages to annual values
    def standardize_wage(row):
        wage = row['wage_rate_of_pay_from']
        unit = row['wage_unit_of_pay'].lower() if pd.notnull(row['wage_unit_of_pay']) else ''
        if unit == 'hour':
            return wage * 2080
        elif unit == 'week':
            return wage * 52
        elif unit == 'month':
            return wage * 12
        elif unit == 'year':
            return wage
        return wage

    df['annual_wage'] = df.apply(standardize_wage, axis=1)

    # Remove outliers in wage data
    df = df[(df['annual_wage'] >= 20000) & (df['annual_wage'] <= 300000)]
    debug_print(f"Rows after filtering out wage outliers: {len(df)}")

    # Retain one row per employer and job code with the average annual wage
    df = df.groupby(['employer_name', 'job_code', 'occupation_title'], as_index=False).agg({
        'annual_wage': 'mean'
    })

    debug_print(f"Rows after grouping by employer and job code: {len(df)}")
    return df

def transform_oews_data(df: pd.DataFrame, relevant_job_codes) -> pd.DataFrame:
    """Transforms the OEWS dataset to include only relevant job codes."""
    debug_print("Starting OEWS data transformation.")
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Standardize SOC codes and remove decimals
    df = df.rename(columns={
        'washington_statewide_occupational_title': 'occupation_title',
        'soc_code': 'job_code',
        'annual_mean_wage': 'avg_local_wage'
    })
    df['job_code'] = df['job_code'].astype(str).str.split('.').str[0].str.strip()

    # Drop rows with null job codes
    df = df.dropna(subset=['job_code'])
    debug_print(f"Rows after removing null job codes: {len(df)}")

    # Convert avg_local_wage to numeric
    df['avg_local_wage'] = pd.to_numeric(df['avg_local_wage'], errors='coerce')
    debug_print(f"Non-numeric values in avg_local_wage converted to NaN.")

    # Drop rows with NaN in avg_local_wage
    df = df.dropna(subset=['avg_local_wage'])
    debug_print(f"Rows after removing NaN values in avg_local_wage: {len(df)}")

    # Filter for relevant job codes
    df = df[df['job_code'].isin(relevant_job_codes)]
    debug_print(f"Rows after filtering OEWS data for relevant job codes: {len(df)}")

    # Remove outliers
    df = df[(df['avg_local_wage'] >= 20000) & (df['avg_local_wage'] <= 300000)]
    debug_print(f"Rows after filtering out wage outliers: {len(df)}")

    relevant_columns = ['occupation_title', 'job_code', 'avg_local_wage']
    df = df[relevant_columns]
    debug_print(f"Rows after retaining relevant columns: {len(df)}")
    return df

def merge_datasets(h1b_df: pd.DataFrame, oews_df: pd.DataFrame) -> pd.DataFrame:
    """Merges the H1B and OEWS datasets on job code."""
    debug_print("Merging H1B and OEWS datasets.")
    merged_df = h1b_df.merge(oews_df, on='job_code', how='inner')
    merged_df['wage_diff'] = pd.to_numeric(merged_df['annual_wage'], errors='coerce') - pd.to_numeric(merged_df['avg_local_wage'], errors='coerce')
    debug_print("Wage difference column added.")
    return merged_df

def save_to_database(df: pd.DataFrame, db_name: str, table_name: str):
    """
    Saves a DataFrame into an SQLite database in the ../data directory.
    """
    # Define the path for the database in the ../data directory
    db_path = os.path.join("../data", db_name)

    # Ensure the ../data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Save the DataFrame to the database
    debug_print(f"Saving data to SQLite database: {db_path}, table: {table_name}")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    debug_print(f"Data successfully saved to SQLite database.")

def visualize_h1b_vs_general_wages(final_df):
    """Visualizes H1B wages vs. general workforce wages."""
    plt.figure(figsize=(12, 6))
    sns.histplot(final_df['annual_wage'], kde=True, color="blue", label="H1B Wages", bins=30)
    sns.histplot(final_df['avg_local_wage'], kde=True, color="orange", label="General Workforce Wages", bins=30)
    plt.axvline(final_df['annual_wage'].mean(), color='blue', linestyle='--', label=f"H1B Mean: ${final_df['annual_wage'].mean():,.0f}")
    plt.axvline(final_df['avg_local_wage'].mean(), color='orange', linestyle='--', label=f"General Workforce Mean: ${final_df['avg_local_wage'].mean():,.0f}")
    plt.title("H1B vs General Workforce Wages in Washington's Tech Sector")
    plt.xlabel("Annual Wage (USD)")
    plt.ylabel("Frequency")
    plt.legend()
    plt.show()

def visualize_biggest_wage_gaps(final_df):
    """Visualizes the biggest wage gaps by occupation."""
    top_gaps = final_df.sort_values(by='wage_diff', ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    sns.barplot(y=top_gaps['occupation_title'], x=top_gaps['wage_diff'], palette="viridis")
    plt.title("Top 10 Tech Occupations with Biggest Wage Gaps (H1B - General)")
    plt.xlabel("Wage Difference (USD)")
    plt.ylabel("Occupation Title")
    plt.show()

def visualize_company_wages(h1b_transformed):
    """Visualizes companies with the highest average H1B wages."""
    company_wages = h1b_transformed.groupby('employer_name')['annual_wage'].mean().reset_index()
    top_companies = company_wages.sort_values(by='annual_wage', ascending=False).head(10)
    plt.figure(figsize=(12, 6))
    sns.barplot(y=top_companies['employer_name'], x=top_companies['annual_wage'], palette="coolwarm")
    plt.title("Top 10 Companies with Highest Average H1B Wages")
    plt.xlabel("Average Annual H1B Wage (USD)")
    plt.ylabel("Company Name")
    plt.show()

def visualize_h1b_role_distribution(h1b_transformed):
    """Visualizes the distribution of H1B workers by role."""
    role_distribution = h1b_transformed['occupation_title'].value_counts().head(10)
    plt.figure(figsize=(10, 6))
    sns.barplot(y=role_distribution.index, x=role_distribution.values, palette="cubehelix")
    plt.title("Top 10 Tech Roles for H1B Workers in Washington")
    plt.xlabel("Number of H1B Workers")
    plt.ylabel("Occupation Title")
    plt.show()

if __name__ == "__main__":
    # URLs for datasets
    h1b_url = "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2023_Q3.xlsx"
    oews_url = "https://esd.wa.gov/media/2861"

    # Step 1: Download the files
    h1b_content = download_file(h1b_url)
    oews_content = download_file(oews_url)

    # Step 2: Parse the files
    h1b_df = parse_file(h1b_content, file_type='excel')
    oews_df = parse_file(oews_content, file_type='excel', sheet_name='Statewide')

    # Step 3: Transform H1B data and retain relevant job codes
    h1b_transformed = transform_h1b_data(h1b_df)
    relevant_job_codes = h1b_transformed['job_code'].unique()

    # Step 4: Transform OEWS data using relevant job codes
    oews_transformed = transform_oews_data(oews_df, relevant_job_codes)

    # Step 5: Merge the datasets
    final_df = merge_datasets(h1b_transformed, oews_transformed)

    # Step 6: Save the datasets
    db_name = "h1b_oews_analysis.db"
    save_to_database(h1b_transformed, db_name, "h1b_roles")
    save_to_database(oews_transformed, db_name, "oews_roles")
    save_to_database(final_df, db_name, "h1b_oews_combined")

    # Visualizations
    visualize_h1b_vs_general_wages(final_df)
    visualize_biggest_wage_gaps(final_df)
    visualize_company_wages(h1b_transformed)
    visualize_h1b_role_distribution(h1b_transformed)

    print(f"Data pipeline complete. Final database saved at: {db_name}")
