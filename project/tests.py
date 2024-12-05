import os
import sqlite3
import pytest

# Constants
DB_NAME = "h1b_oews_analysis.db"
DB_PATH = os.path.join("../data", DB_NAME)
EXPECTED_TABLES = [
    "h1b_microsoft_roles",
    "oews_microsoft_roles",
    "h1b_oews_combined",
]

@pytest.fixture(scope="function")
def clean_environment():
    """
    Fixture to ensure a clean environment before and after each test.
    """
    # Clean up after the test
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

def test_database_exists():
    """
    Check if the database file exists.
    """
    assert os.path.exists(DB_PATH), f"Database file '{DB_PATH}' was not created by the pipeline."

def test_expected_tables_exist():
    """
    Validate that the expected tables are created in the database.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        for table in EXPECTED_TABLES:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            result = cursor.fetchone()
            assert result is not None, f"Table '{table}' was not created in the database."

def test_combined_table_columns():
    """
    Validate that the combined table has the expected structure.
    """
    expected_columns = ['occupation_title_x', 'employer_name', 'job_code', 
                        'annual_wage', 'occupation_title_y', 
                        'avg_local_wage', 'wage_diff']
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('h1b_oews_combined');")
        columns = [row[1] for row in cursor.fetchall()]
        assert set(expected_columns) <= set(columns), "Combined table does not have the expected columns."

def test_no_null_job_code():
    """
    Check for null values in the job_code column of all relevant tables.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        for table in EXPECTED_TABLES:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE job_code IS NULL;")
            null_count = cursor.fetchone()[0]
            assert null_count == 0, f"Table '{table}' contains {null_count} null values in the 'job_code' column."

def test_no_null_critical_columns():
    """
    Check for null values in critical columns of all relevant tables.
    """
    critical_columns = {
        "h1b_microsoft_roles": ["job_code", "annual_wage"],
        "oews_microsoft_roles": ["job_code", "avg_local_wage"],
        "h1b_oews_combined": ["job_code", "annual_wage", "avg_local_wage", "wage_diff"]
    }
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        for table, columns in critical_columns.items():
            for column in columns:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL;")
                null_count = cursor.fetchone()[0]
                assert null_count == 0, f"Table '{table}' contains {null_count} null values in the '{column}' column."

def test_wage_diff_computation():
    """
    Validate that the wage_diff column is computed correctly in the combined table.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT annual_wage, avg_local_wage, wage_diff 
            FROM h1b_oews_combined
            WHERE wage_diff IS NOT NULL;
            """
        )
        rows = cursor.fetchall()
        for annual_wage, avg_local_wage, wage_diff in rows:
            expected_diff = annual_wage - avg_local_wage
            assert abs(wage_diff - expected_diff) < 1e-6, (
                f"Incorrect wage_diff calculation. Expected {expected_diff}, got {wage_diff}."
            )

def test_valid_wage_range():
    """
    Ensure that annual wages and avg_local_wage fall within expected ranges.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM h1b_microsoft_roles WHERE annual_wage < 20000 OR annual_wage > 300000;")
        out_of_range_h1b = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM oews_microsoft_roles WHERE avg_local_wage < 20000 OR avg_local_wage > 300000;")
        out_of_range_oews = cursor.fetchone()[0]
        assert out_of_range_h1b == 0, f"H1B table contains {out_of_range_h1b} rows with wages out of range."
        assert out_of_range_oews == 0, f"OEWS table contains {out_of_range_oews} rows with wages out of range."

def test_unique_job_codes():
    """
    Ensure that job codes in the combined table are unique.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT job_code, COUNT(*) FROM h1b_oews_combined GROUP BY job_code HAVING COUNT(*) > 1;")
        duplicate_job_codes = cursor.fetchall()
        assert not duplicate_job_codes, f"Duplicate job codes found in combined table: {duplicate_job_codes}"

if __name__ == "__main__":
    pytest.main(["-v", __file__])
