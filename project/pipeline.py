import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class DataPipeline:

    def download_file_using_selenium(self, page_url, file_path, download_dir="data", max_retries=3, timeout=10):
        try:
            # Initialize the WebDriver
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

            # Open the specified page URL
            driver.get(page_url)

            # Get page source and close the driver
            html = driver.page_source
            driver.quit()

            # Parse the page source with BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            start_of_url = "https://www.dol.gov"

            # Construct the absolute URL for the desired file
            absolute_url = start_of_url + file_path

            # Ensure the download directory exists
            os.makedirs(download_dir, exist_ok=True)
            file_name = file_path.split("/")[-1]
            file_save_path = os.path.join(download_dir, file_name)

            # Retry logic for downloading the file
            for attempt in range(max_retries):
                try:
                    print(f"Attempt {attempt + 1} to download the file.")
                    response = requests.get(absolute_url, stream=True, timeout=timeout)
                    
                    # Check if the response is successful
                    if response.status_code == 200:
                        with open(file_save_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        # Check if the file is actually an Excel file by verifying the size
                        if os.path.getsize(file_save_path) > 0:
                            print(f"File downloaded successfully: {file_save_path}")
                            return  # Exit the function after a successful download
                        else:
                            print("Downloaded file is empty. Retrying...")
                            os.remove(file_save_path)
                    else:
                        print(f"Failed to download file. Status code: {response.status_code}")
                        
                except requests.exceptions.Timeout:
                    print(f"Timeout on attempt {attempt + 1}. Retrying...")
                except requests.exceptions.RequestException as e:
                    print(f"Request failed on attempt {attempt + 1}: {e}")
                    
                # Wait before the next attempt
                time.sleep(2)

            raise Exception("Failed to download the file after multiple attempts.")

        except Exception as e:
            print(f"An error occurred: {e}")

   
    def download_file_simple(self, url, file_name, download_dir="data", timeout=10, max_retries=3):
        try:
            # Set up a session with retry strategy
            session = requests.Session()
            retries = Retry(
                total=max_retries,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504]
            )
            session.mount('https://', HTTPAdapter(max_retries=retries))

            # Send GET request
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            os.makedirs(download_dir, exist_ok=True)
            file_save_path = os.path.join(download_dir, file_name)

            # Save the content to a file
            with open(file_save_path, 'wb') as file:
                file.write(response.content)

            print(f"Download successful: {file_save_path}")
            return True

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred: {req_err}")

        return False

    def download_h1b_data(self):
        page_url = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
        file_path = "/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2024_Q3.xlsx"
        self.download_file_using_selenium(page_url, file_path)
            
    def download_washington_state_employment(self):
        url = "https://esd.wa.gov/media/2861"
        file_name = "state_employement.csv"
        self.download_file_simple(url, file_name)
            

    def load_lca_data(self, filepath):
        return pd.read_excel(filepath)

    def load_occupation_data(self, filepath):
        return pd.read_csv(filepath)     
       
    def filter_lca_data(self, df):
        return df[
            (df['EMPLOYER_NAME'] == 'Microsoft Corporation') &
            (df['WORKSITE_CITY'].str.contains('Seattle', case=False, na=False))
        ][[
            'EMPLOYER_NAME', 'WORKSITE_CITY', 'BASE_SALARY', 'JOB_TITLE',
            'WAGE_RATE', 'WAGE_LEVEL', 'WORKSITE_COUNTY', 'WORKSITE_POSTAL_CODE'
        ]]

    def filter_occupation_data(self, df):
        return df[
            df['AREA_NAME'].str.contains('Seattle-Bellevue-Everett', case=False, na=False)
        ][[
            'OCCUPATION_CODE', 'OCCUPATION_TITLE', 'AREA_NAME',
            'WAGE_MEAN', 'WAGE_MEDIAN', 'EMPLOYMENT_COUNT'
        ]]

    def merge_datasets(self, lca_df, occupation_df):
        return pd.merge(
            lca_df, occupation_df,
            how='inner',
            left_on='JOB_TITLE', right_on='OCCUPATION_TITLE'
        )

    def save_to_database(self, df, table_name, db_engine):
        df.to_sql(table_name, con=db_engine, if_exists='replace', index=False)

    def main(self):
        # Define file paths
        lca_file_path = 'data/LCA_Disclosure_Data_FY2024_Q3.xlsx'
        occupation_file_path = 'data/state_employement.csv'

        # Check if files are already downloaded; if not, download them
        if not os.path.exists(lca_file_path):
            print("LCA file not found. Downloading LCA data...")
            self.download_h1b_data()
        else:
            print("LCA file already exists. Skipping download.")

        if not os.path.exists(occupation_file_path):
            print("Occupation data file not found. Downloading occupation data...")
            self.download_washington_state_employment()
        else:
            print("Occupation data file already exists. Skipping download.")

        # Load datasets only if files exist
        if os.path.exists(lca_file_path) and os.path.exists(occupation_file_path):
            # Load datasets
            lca_df = self.load_lca_data(lca_file_path)
            occupation_df = self.load_occupation_data(occupation_file_path)
            
            # Filter data
            filtered_lca = self.filter_lca_data(lca_df)
            filtered_occupation = self.filter_occupation_data(occupation_df)
            
            # Merge datasets
            merged_data = self.merge_datasets(filtered_lca, filtered_occupation)
            
            # Database connection (replace with your database configuration)
            engine = create_engine('sqlite:///integrated_data.db')
            
            # Save filtered and merged data to database
            self.save_to_database(filtered_lca, 'lca_data', engine)
            self.save_to_database(filtered_occupation, 'occupation_data', engine)
            self.save_to_database(merged_data, 'merged_data', engine)
        else:
            print("Error: One or both data files are missing, unable to load data.")

# Run the pipeline
if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.main()
