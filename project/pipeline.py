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
#selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
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


    def selenium_setup(self):
         driver=webdriver.Chrome(ChromeDriverManager.install())
         driver.get("http://www.python.org")
   
    def download_h1b_data(self):
            #"""Download H1B visa data from DOL website with retry logic and timeout."""
            url = "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/H-1B_Disclosure_Data_FY2023_Q4.xlsx"
            temp_file = os.path.join(self.data_dir, "h1b_raw.xlsx")
        
            max_retries = 3  # Number of retries in case of connection issues
            for attempt in range(max_retries):
                try:
                    logging.info("Attempting to download H1B data...")

                    # Use timeout and disable SSL verification if needed
                    response = requests.get(url, timeout=10, verify=True)  # set verify=True if SSL is necessary

                    # Check for successful response
                    if response.status_code == 200:
                        with open(temp_file, 'wb') as f:
                            f.write(response.content)
                        break
                    else:
                        logging.warning(f"Unexpected status code {response.status_code} on attempt {attempt + 1}")

                except requests.exceptions.RequestException as e:
                    logging.warning(f"Attempt {attempt + 1} failed: {e}")
                    time.sleep(5)  # Wait before retrying
            else:
                logging.error("Failed to download H1B data after multiple attempts.")
                raise Exception(f"failed download {response.status_code} ")

            # Process downloaded file
            df = pd.read_excel(temp_file, engine='openpyxl')
            microsoft_data = df[
                (df['EMPLOYER_NAME'].str.contains('MICROSOFT', case=False, na=False)) &
                (df['WORKSITE_CITY'].isin(['SEATTLE', 'REDMOND', 'BELLEVUE']))
            ]
            return microsoft_data


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
        """Execute the complete pipeline"""
        try:
            logging.info("Starting pipeline execution...")
            
            # Download data
            h1b_data = self.download_h1b_data()
            wage_data = self.download_washington_wage_data()
            
            # Clean data
            h1b_clean, wage_clean = self.clean_data(h1b_data, wage_data)
            
            # Save to database
            self.save_to_database(h1b_clean, wage_clean)
            
            logging.info("Pipeline completed successfully")
            
        except Exception as e:
            logging.error(f"Pipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_pipeline()