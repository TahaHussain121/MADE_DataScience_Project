import os
import pandas as pd
import sqlite3
from io import BytesIO
import requests
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import ttest_ind
import numpy as np


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
    """Transforms and filters the H1B dataset for specific companies."""
    debug_print("Starting H1B data transformation.")
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    debug_print(f"Initial H1B dataset rows: {len(df)}")

    # Filter for CASE_STATUS == "CERTIFIED"
    df = df[df['case_status'].str.strip().str.upper() == 'CERTIFIED']
    debug_print(f"Rows after filtering for certified cases: {len(df)}")

    # Filter for specific companies
    target_companies = ["microsoft", "google llc",  "amazon web services"]
    df = df[df['employer_name'].str.contains('|'.join(target_companies), case=False, na=False)]
    debug_print(f"Rows after filtering for target companies: {len(df)}")

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
    #df = df[(df['avg_local_wage'] >= 20000) & (df['avg_local_wage'] <= 300000)]
    #debug_print(f"Rows after filtering out wage outliers: {len(df)}")

    relevant_columns = ['occupation_title', 'job_code', 'avg_local_wage']
    df = df[relevant_columns]
    debug_print(f"Rows after retaining relevant columns: {len(df)}")
    return df

def merge_datasets(h1b_df: pd.DataFrame, oews_df: pd.DataFrame) -> pd.DataFrame:
    """Merges the H1B and OEWS datasets on job code, keeping only the H1B occupation_title."""
    debug_print("Merging H1B and OEWS datasets.")
    # Select only the necessary columns from each dataset
    h1b_selected = h1b_df[['job_code', 'occupation_title', 'annual_wage','employer_name']]
    oews_selected = oews_df[['job_code', 'avg_local_wage']]
    
    # Merge on job_code
    merged_df = h1b_selected.merge(oews_selected, on='job_code', how='inner')
    
    # Calculate the wage difference
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

def correlation_analysis(df, output_path):
    """Performs and visualizes correlation analysis."""
    correlation = df['annual_wage'].corr(df['avg_local_wage'])
    print(f"Correlation between H1B wages and general workforce wages: {correlation:.2f}")

    # Visualization
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=df['annual_wage'], y=df['avg_local_wage'], alpha=0.7)
    plt.title(f"Correlation Between H1B and General Workforce Wages (r = {correlation:.2f})")
    plt.xlabel("H1B Annual Wage (USD)")
    plt.ylabel("General Workforce Annual Wage (USD)")
    plt.savefig(os.path.join(output_path, "correlation_analysis.png"), dpi=300)
    plt.show()
    plt.close()
    

def t_test_analysis(df, output_path):
    """Performs and visualizes a t-test between H1B and general workforce wages."""
    t_stat, p_value = ttest_ind(df['annual_wage'], df['avg_local_wage'], equal_var=False)
    print(f"T-Test Statistic: {t_stat:.2f}, P-Value: {p_value:.4f}")

    # Visualization
    plt.figure(figsize=(8, 6))
    sns.kdeplot(df['annual_wage'], label="H1B Wages", fill=True, alpha=0.5)
    sns.kdeplot(df['avg_local_wage'], label="General Workforce Wages", fill=True, alpha=0.5)
    plt.title("Distribution of Wages with T-Test Results")
    plt.xlabel("Annual Wage (USD)")
    plt.ylabel("Density")
    plt.legend()
    plt.savefig(os.path.join(output_path, "t_test_analysis.png"), dpi=300)
    plt.show()
    plt.close()
    
def create_visualizations(final_df, output_dir):
    """Generates and saves visualizations based on the merged dataset."""
    os.makedirs(output_dir, exist_ok=True)

    # Focus on specific companies
    companies_of_interest = ["microsoft", "google llc", "amazon web services"]
    filtered_df = final_df[final_df['employer_name'].str.contains('|'.join(companies_of_interest), case=False)]

    # Get top 5 most common roles across all companies
    top_5_roles = filtered_df['occupation_title'].value_counts().head(5).index

    # Filter data for top 5 roles only
    top_roles_df = filtered_df[filtered_df['occupation_title'].isin(top_5_roles)]

    # 1. Average H1B vs. General Workforce Wages per Company and Role
    plt.figure(figsize=(15, 8))

    # Calculate positions for grouped bars
    width = 0.35
    x = np.arange(len(top_5_roles))

    # Create grouped bar plot for each company
    for i, company in enumerate(companies_of_interest):
        company_data = top_roles_df[top_roles_df['employer_name'].str.contains(company, case=False)]

        # Initialize arrays with NaN to match the length of top_5_roles
        avg_wages = np.full(len(top_5_roles), np.nan)
        local_wages = np.full(len(top_5_roles), np.nan)

        # Fill in the data where available
        for j, role in enumerate(top_5_roles):
            role_data = company_data[company_data['occupation_title'] == role]
            if not role_data.empty:
                avg_wages[j] = role_data['annual_wage'].mean()
                local_wages[j] = role_data['avg_local_wage'].mean()

        # Only plot non-NaN values
        mask = ~np.isnan(avg_wages)
        if np.any(mask):
            offset = (i - 1) * width
            plt.bar(x[mask] + offset, avg_wages[mask], width,
                   label=f'{company} H1B Wages')
            plt.bar(x[mask] + offset + width/3, local_wages[mask], width/3,
                   label=f'{company} Local Wages', alpha=0.7)

    plt.xlabel('Occupation Title')
    plt.ylabel('Average Wage (USD)')
    plt.title('H1B vs Local Wages by Company and Role\n(Top 5 Most Common Roles)')
    plt.xticks(x, top_5_roles, rotation=45, ha='right')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "role_wage_comparison.png"),
                dpi=300, bbox_inches='tight')
    plt.close()

    # 2. Largest Wage Differences by Job Title
    wage_diff_by_title = filtered_df.groupby('occupation_title')['wage_diff'].mean().sort_values(ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    wage_diff_by_title.plot(kind='barh', color='skyblue')
    plt.title("Top 10 Job Titles with Largest Wage Differences")
    plt.xlabel("Average Wage Difference (USD)")
    plt.ylabel("Job Title")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "wage_diff_by_job_title.png"), dpi=300)
    plt.close()

    # 3. Consistency of Wage Premiums Across Job Categories
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=filtered_df, x="employer_name", y="wage_diff", notch=True, palette="pastel")
    plt.title("Distribution of Wage Differences by Company")
    plt.ylabel("Wage Difference (USD)")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "wage_diff_distribution_by_company.png"), dpi=300)
    plt.close()

    # 4. Concentration of H1B Positions by Role
    role_counts = filtered_df['occupation_title'].value_counts().head(5)
    plt.figure(figsize=(8, 6))
    role_counts.plot(kind='pie', autopct='%1.1f%%', startangle=140, colormap="viridis", wedgeprops={"edgecolor":"k"})
    plt.title("Top 5 Roles for H1B Workers")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "h1b_role_distribution.png"), dpi=300)
    plt.close()

    print(f"Visualizations saved to {output_dir}")


    print(f"Visualizations saved to {output_dir}")
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
    output_dir = "../data"
    save_to_database(h1b_transformed, db_name, "h1b_roles")
    save_to_database(oews_transformed, db_name, "oews_roles")
    save_to_database(final_df, db_name, "h1b_oews_combined")

    # Statistical Analysis and Visualizations
    correlation_analysis(final_df, output_dir)
    t_test_analysis(final_df, output_dir)
    create_visualizations(final_df, output_dir)
    print(f"Data pipeline complete. Final database and visualizations saved in: {output_dir}")
