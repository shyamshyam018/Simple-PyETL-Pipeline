import pandas as pd
import sqlite3
import schedule
import time
from datetime import datetime
import logging
import os
from tabulate import tabulate  # Used for clean table printing in the terminal

# --- Configuration ---
DB_NAME = 'customer_data.db'
SOURCE_TABLE = 'raw_customers_staging'
TARGET_TABLE = 'dim_customers_final'

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s %(asctime)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Sample Data ---
SAMPLE_DATA = [
    ('CG-12520', 'Claire Gute', 'Consumer', 'Kentucky', 'Henderson'),
    ('DV-13045', 'Darrin Van Huff', 'Corporate', 'California', 'Los Angeles'),
    ('SO-20335', 'Sean O\'Donnell', 'Consumer', 'Florida', 'Fort Lauderdale'),
    ('BH-11710', 'Brosina Hoffman', 'Consumer', 'California', 'Los Angeles'),
    ('SG-19345', 'Samantha Green', 'Corporate', 'California', 'Los Angeles'), # Duplicate for testing
    ('SG-19345', 'Samantha Green', 'Corporate', 'California', 'Los Angeles'), # Duplicate for testing
]
COLUMNS = ['id', 'name', 'segment', 'state', 'city']


# --- Core Functions ---

def get_db_connection():
    """Returns a connection object to the SQLite database."""
    return sqlite3.connect(DB_NAME)

def display_table(table_name, conn=None, close_conn=True, title_override=None):
    """Fetches and displays the contents of a specified database table."""
    conn_provided = conn is not None
    if conn is None:
        conn = get_db_connection() # Fixed: using sqlite3.connect
    try:
        # Read data into a pandas DataFrame
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        title = title_override if title_override else f"Table: {table_name} ({len(df)} rows)"

        print("\n" + "="*70)
        print(f"| {title.center(66)} |")
        print("="*70)

        if df.empty:
            print(f"| {'Table is empty.'.center(66)} |")
            print("="*70)
        else:
            # Use tabulate for a clean terminal output
            print(tabulate(df, headers='keys', tablefmt='fancy_grid', showindex=False))
            print("="*70)

    except pd.io.sql.DatabaseError as e:
        logging.error(f"Could not read table '{table_name}'. Error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while displaying table: {e}")
    finally:
        # Close connection only if we opened it inside this function
        if close_conn and not conn_provided:
            conn.close()


def create_sample_database(conn):
    """
    Creates the staging table and populates it with SAMPLE_DATA.
    Also ensures the target table exists but is empty initially.
    """
    logging.info(f"Setting up database '{DB_NAME}' with sample data.")

    # 1. Create Staging Table (Source)
    conn.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
    create_staging_sql = f"""
    CREATE TABLE {SOURCE_TABLE} (
        id TEXT NOT NULL,
        name TEXT,
        segment TEXT,
        state TEXT,
        city TEXT
    );
    """
    conn.execute(create_staging_sql)
    logging.info(f"Table '{SOURCE_TABLE}' created.")

    # 2. Insert Sample Data
    placeholders = ', '.join(['?'] * len(COLUMNS))
    insert_sql = f"INSERT INTO {SOURCE_TABLE} ({', '.join(COLUMNS)}) VALUES ({placeholders})"
    conn.executemany(insert_sql, SAMPLE_DATA)
    conn.commit()
    logging.info(f"Inserted {len(SAMPLE_DATA)} rows into '{SOURCE_TABLE}'. (Includes duplicates for testing)")

    # 3. Create Final Table (Target) structure if it doesn't exist
    create_final_sql = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id TEXT PRIMARY KEY,
        name TEXT,
        segment TEXT,
        state TEXT,
        city TEXT,
        last_updated TEXT
    );
    """
    conn.execute(create_final_sql)
    conn.commit()
    logging.info(f"Target table '{TARGET_TABLE}' structure ensured.")


def extract_and_load_raw_data():
    """
    In a real-world scenario, this would extract data from an external source.
    In this script, it confirms data readiness or loads sample data if needed.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check if the staging table is empty
        count_query = cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]

        if count_query == 0:
            logging.warning(f"Staging table '{SOURCE_TABLE}' is empty. Re-loading sample data.")
            create_sample_database(conn)
            
        logging.info(f"--- Extract & Load Phase Completed. {count_query} rows found in staging. ---")
        display_table(SOURCE_TABLE, conn=conn, close_conn=False)

    except sqlite3.OperationalError:
        # This occurs if the table does not exist
        logging.error(f"Staging table '{SOURCE_TABLE}' does not exist. Initializing database with sample data.")
        create_sample_database(conn)
        display_table(SOURCE_TABLE, conn=conn, close_conn=False)
    finally:
        conn.close()


def transform_data():
    """
    Transforms the data by de-duplicating records from the staging table
    and loading the clean result into the final dimension table.
    Includes a schema check to prevent 'no column' errors on old databases.
    """
    logging.info(f"--- Starting Transformation Phase at {datetime.now()} ---")
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # --- SCHEMA CHECK/MIGRATION ---
        # 1. Check if the 'last_updated' column exists (to handle pre-existing databases without the column)
        try:
            # Query the table schema using PRAGMA table_info
            schema_info = cursor.execute(f"PRAGMA table_info({TARGET_TABLE})").fetchall()
            column_names = [col[1] for col in schema_info]
            
            if 'last_updated' not in column_names:
                logging.warning(f"Column 'last_updated' missing from '{TARGET_TABLE}'. Attempting to add it.")
                alter_sql = f"ALTER TABLE {TARGET_TABLE} ADD COLUMN last_updated TEXT;"
                cursor.execute(alter_sql)
                conn.commit()
                logging.info(f"Column 'last_updated' successfully added to '{TARGET_TABLE}'.")
        except sqlite3.OperationalError:
            # This handles the case where the TARGET_TABLE itself doesn't exist yet, 
            # which would be caught and handled by extract_and_load_raw_data calling create_sample_database.
            pass
        # --- SCHEMA CHECK/MIGRATION END ---


        # SQL to perform de-duplication: select the unique ID and the rest of the data.
        # SQLite's DISTINCT ON is not supported, so we use a grouping method.
        # Since we just want unique IDs, we can use GROUP BY to select one record per ID.
        transform_sql = f"""
        INSERT OR REPLACE INTO {TARGET_TABLE} (id, name, segment, state, city, last_updated)
        SELECT
            id,
            name,
            segment,
            state,
            city,
            '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' AS last_updated
        FROM {SOURCE_TABLE}
        GROUP BY id;
        """
        
        cursor.execute(f"DELETE FROM {TARGET_TABLE}") # Truncate the target table before loading
        logging.info(f"Target table '{TARGET_TABLE}' truncated.")

        cursor.execute(transform_sql)
        row_count = cursor.rowcount  # FIXED: Changed conn.rowcount to cursor.rowcount
        conn.commit()

        logging.info(f"--- Transform Phase Completed. Loaded {row_count} unique records into '{TARGET_TABLE}'. ---")
        display_table(TARGET_TABLE, conn=conn, close_conn=False)

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
    finally:
        conn.close()


def run_etl_pipeline():
    """Orchestrates the full ETL process."""
    logging.info("==================================================")
    logging.info(f"STARTING ETL RUN: {datetime.now()}")
    logging.info("==================================================")
    
    # 1. Extract & Load (Staging)
    extract_and_load_raw_data()
    
    # 2. Transform & Load (Final)
    transform_data()
    
    logging.info("==================================================")
    logging.info("ETL RUN FINISHED SUCCESSFULLY.")
    logging.info("==================================================")


def get_user_choice_and_initialize():
    """
    Prompts the user to choose between using the existing DB or sample data.
    """
    db_exists = os.path.exists(DB_NAME)

    if db_exists:
        logging.info(f"Database file '{DB_NAME}' found.")
        print(f"\n--- Database Initialization ---")
        print(f"1. Use existing database '{DB_NAME}' (Recommended for continuous runs).")
        print(f"2. Overwrite '{DB_NAME}' and initialize with Sample Data (Use for testing/reset).")

        while True:
            choice = input("Enter your choice (1 or 2): ").strip()
            if choice == '1':
                logging.info("User chose to use the existing database.")
                return
            elif choice == '2':
                logging.warning(f"User chose to overwrite '{DB_NAME}' with sample data.")
                # We initialize here so the subsequent ETL run finds the correct setup
                conn = get_db_connection()
                try:
                    create_sample_database(conn)
                finally:
                    conn.close()
                return
            else:
                print("Invalid choice. Please enter '1' or '2'.")
    else:
        logging.warning(f"Database file '{DB_NAME}' not found.")
        print(f"\n--- Database Initialization ---")
        print(f"Automatically creating database '{DB_NAME}' and initializing with sample data.")
        conn = get_db_connection()
        try:
            create_sample_database(conn)
        finally:
            conn.close()


if __name__ == '__main__':
    # 1. Initialize DB based on user input
    get_user_choice_and_initialize()

    # 2. Run the ETL pipeline immediately
    run_etl_pipeline()

    # 3. Schedule the ETL to run periodically
    logging.info("ETL process is now scheduled to run every 10 seconds. Press Ctrl+C to stop.")
    schedule.every(10).seconds.do(run_etl_pipeline)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("ETL Scheduler stopped by user.")
