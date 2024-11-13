import pandas as pd
import requests
import yaml
import time
import os
import mimetypes
import logging
from datetime import datetime
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# Set up logging
logging.basicConfig(
    filename='data/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataPipeline:
    def __init__(self):
        self.data_dir = "data"
        self.db_path = os.path.join(self.data_dir, "microsoft_compensation.db")
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)


    def download_file_Using_Selenium(page_url, file_path, download_dir="downloads", max_retries=3, timeout=10):
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
            file_name = file_path.split("/")[-1]  # Extract the file name from the path
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
                        print(f"File downloaded successfully: {file_save_path}")
                        return  # Exit the function after a successful download
                    else:
                        print(f"Failed to download file. Status code: {response.status_code}")
                        
                except requests.exceptions.Timeout:
                    print(f"Timeout on attempt {attempt + 1}. Retrying...")
                except requests.exceptions.RequestException as e:
                    print(f"Request failed on attempt {attempt + 1}: {e}")
                    
                # Wait before the next attempt
                time.sleep(2)

            # Raise an exception if all attempts fail
            raise Exception("Failed to download the file after multiple attempts.")

        except Exception as e:
            print(f"An error occurred: {e}")

   
    def download_file_simple(url, save_path, timeout=10, max_retries=3):
       
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
            response.raise_for_status()  # Check for HTTP errors

            # Save the content to a file
            with open(save_path, 'wb') as file:
                file.write(response.content)

            print(f"Download successful: {save_path}")
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
            #"""Download H1B visa data from DOL website with retry logic and timeout."""
            page_url = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
            file_path = "/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2024_Q3.xlsx"
            self.download_file_Using_Selenium(page_url, file_path)
            
    def download_washington_state_employment(self):
            #"""Download H1B visa data from DOL website with retry logic and timeout."""
            url = "https://esd.wa.gov/media/2861"
            save_path = os.path.join(os.getcwd(), "dataset.csv")
            download_file_simple(url, save_path)
            
    def download_h1b_data(self):
            #"""Download H1B visa data from DOL website with retry logic and timeout."""
            page_url = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
            file_path = "/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2024_Q3.xlsx"
            self.download_file_from_page(page_url, file_path)
            


    def clean_data(self, h1b_df, wage_df):
        """Clean and transform the datasets"""
        try:
            logging.info("Cleaning datasets...")
            
            # Clean H1B data
            h1b_df['BASE_SALARY'] = pd.to_numeric(h1b_df['BASE_SALARY'], errors='coerce')
            h1b_df['WAGE_LEVEL'] = h1b_df['WAGE_LEVEL'].fillna('Not Specified')
            
            # Clean wage data
            wage_df['WAGE_MEDIAN'] = pd.to_numeric(wage_df['WAGE_MEDIAN'], errors='coerce')
            
            # Standardize location names
            h1b_df['WORKSITE_CITY'] = h1b_df['WORKSITE_CITY'].str.upper()
            
            return h1b_df, wage_df
            
        except Exception as e:
            logging.error(f"Error cleaning data: {str(e)}")
            raise

    def save_to_database(self, h1b_df, wage_df):
        """Save processed data to SQLite database"""
        try:
            logging.info("Saving to database...")
            
            # Save to SQLite
            h1b_df.to_sql('microsoft_h1b', self.engine, if_exists='replace', index=False)
            wage_df.to_sql('washington_wages', self.engine, if_exists='replace', index=False)
            
            # Create view combining both tables
            query = """
            CREATE VIEW IF NOT EXISTS compensation_analysis AS
            SELECT 
                h.JOB_TITLE,
                h.BASE_SALARY,
                h.WAGE_LEVEL,
                w.WAGE_MEDIAN as AREA_MEDIAN_WAGE,
                w.EMPLOYMENT_COUNT as AREA_EMPLOYMENT
            FROM microsoft_h1b h
            LEFT JOIN washington_wages w
            ON h.JOB_TITLE = w.OCCUPATION_TITLE
            """
            
            with self.engine.connect() as conn:
                conn.execute(query)
                
            logging.info("Data successfully saved to database")
            
        except Exception as e:
            logging.error(f"Error saving to database: {str(e)}")
            raise

    def run_pipeline(self):
        
        # """Execute the complete pipeline"""
        # try:
        #     logging.info("Starting pipeline execution...")
            
        #     # Download data
        #     h1b_data = self.download_h1b_data()
        #     wage_data = self.download_washington_wage_data()
            
        #     # Clean data
        #     h1b_clean, wage_clean = self.clean_data(h1b_data, wage_data)
            
        #     # Save to database
        #     self.save_to_database(h1b_clean, wage_clean)
            
        #     logging.info("Pipeline completed successfully")
            
        # except Exception as e:
        #     logging.error(f"Pipeline failed: {str(e)}")
        #     raise

        if __name__ == "__main__":
            pipeline = DataPipeline()
            pipeline.run_pipeline()