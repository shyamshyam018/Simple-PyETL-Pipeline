import pandas as pd
import sqlite3
import schedule
import time
from datetime import datetime
import logging
import os
from tabulate import tabulate # Used for clean table printing in the terminal

# --- Configuration ---
DB_NAME = 'customer_data.db'
SOURCE_TABLE = 'raw_customers_staging'
TARGET_TABLE = 'dim_customers_final'

# Setup Logging
# We use a custom format and print to both file and console
logging.basicConfig(level=logging.INFO, 
                    format='[%(levelname)s] %(asctime)s - %(message)s')

# --- Sample Data ---
SAMPLE_DATA = [
    ('CG-12520', 'Claire Gute', 'Consumer', 'Kentucky', 'Henderson'),
    ('DV-13045', 'Darrin Van Huff', 'Corporate', 'California', 'Los Angeles'),
    ('SO-20335', 'Sean O\'Donnell', 'Consumer', 'Florida', 'Fort Lauderdale'),
    ('BH-11710', 'Brosina Hoffman', 'Consumer', 'California', 'Los Angeles'),
    ('SG-19345', 'Samantha Green', 'Corporate', 'California', 'Los Angeles'), # Duplicate for testing
]
COLUMNS = ['id', 'name', 'segment', 'state', 'city']

# -----------------------------------------------------------------
## HELPER FUNCTION: Table Display
# -----------------------------------------------------------------

def display_table(table_name, conn=None, close_conn=True, title_override=None):
    """Fetches data and prints it cleanly to the terminal."""
    if conn is None:
        conn = sqlite3.connect(DB_NAME)
        
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        title = title_override if title_override else f"Table: {table_name} ({len(df)} rows)"
        
        print("\n" + "="*50)
        print(f"| {title.center(46)} |")
        print("="*50)
        
        if df.empty:
            print(f"| {'Table is empty.'.center(46)} |")
        else:
            # Using tabulate to format the DataFrame nicely
            print(tabulate(df, headers='keys', tablefmt='fancy_grid', showindex=False))
            
    except pd.io.sql.DatabaseError:
        print(f"\n[ERROR] Table '{table_name}' does not exist or cannot be read.")
    finally:
        if close_conn:
            conn.close()

# -----------------------------------------------------------------
## 1. EXTRACT and LOAD (E & L) Phase: The Data Ingestion
# -----------------------------------------------------------------

def extract_and_load_raw_data():
    """Simulates Extracting data and Loading it into the Staging Table."""
    logging.info(f"--- Starting E & L: Loading data into {SOURCE_TABLE} (Staging) ---")
    
    try:
        conn = sqlite3.connect(DB_NAME)
        
        # 1. EXTRACTION (E): From source list to Pandas DataFrame (In-memory)
        print("\n[STEP 1.1 - EXTRACTION] Reading raw data into memory...")
        df_raw = pd.DataFrame(SAMPLE_DATA, columns=COLUMNS)
        display_table("RAW DATA EXTRACTED", conn, False, f"Raw Source Data ({len(df_raw)} records)")
        
        # 2. LOADING (L): From Pandas to SQLite Staging Table
        print(f"\n[STEP 1.2 - LOADING] Writing {len(df_raw)} records to {SOURCE_TABLE}...")
        df_raw.to_sql(SOURCE_TABLE, conn, if_exists='replace', index=False)
        logging.info(f"E & L Successful. Data is now staged.")
        
        # Visualize the staging table after loading
        display_table(SOURCE_TABLE, conn, False, f"STAGING TABLE: {SOURCE_TABLE}")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"E & L Failed: {e}", exc_info=True)
        print(f"\n[FAILURE] Check logs for details. E & L failed.")
        return False

# -----------------------------------------------------------------
## 2. TRANSFORM (T) Phase: The Manufacturing Factory
# -----------------------------------------------------------------

def transform_and_load_dimensions():
    """Transforms data from staging and loads the clean data to the final warehouse."""
    logging.info(f"--- Starting T: Transforming data from {SOURCE_TABLE} ---")
    
    try:
        conn = sqlite3.connect(DB_NAME)
        
        # 1. Extract from Staging
        query = f"SELECT * FROM {SOURCE_TABLE}"
        df_staging = pd.read_sql(query, conn)
        print(f"\n[STEP 2.1 - EXTRACT] Read {len(df_staging)} records from staging for transformation.")

        # --- Transformation Logic Visualization ---
        
        # T1: Derived Column - Creating a simple 'is_corporate' flag
        print("\n[STEP 2.2 - TRANSFORM] Applying Business Rule: Deriving 'is_corporate' flag.")
        df_staging['is_corporate'] = df_staging['segment'].apply(lambda x: 1 if x == 'Corporate' else 0)
        
        # T2: Data Quality/Cleaning - Capitalizing the first letter of cities
        print("[STEP 2.3 - TRANSFORM] Applying Data Quality: Capitalizing city names.")
        df_staging['city'] = df_staging['city'].str.title()
        
        # T3: Aggregation/Dimension Modeling - Removing duplicates (Unique Customer IDs)
        initial_count = len(df_staging)
        df_clean = df_staging.drop_duplicates(subset=['id'], keep='first')
        removed_count = initial_count - len(df_clean)
        
        print(f"[STEP 2.4 - TRANSFORM] Deduplication: Removed {removed_count} duplicate customer IDs.")

        # Select the final columns (Simulating Schema Enforcement/Evolution)
        df_final = df_clean[['id', 'name', 'segment', 'state', 'city', 'is_corporate']]
        
        display_table("TRANSFORMED DATA (In-Memory)", conn, False, f"Transformed Data Preview ({len(df_final)} records)")
        
        # 2. Loading (L) into the Target Dimension Table (Data Warehouse)
        print(f"\n[STEP 2.5 - LOADING] Writing {len(df_final)} final records to {TARGET_TABLE}...")
        df_final.to_sql(TARGET_TABLE, conn, if_exists='replace', index=False)
        
        logging.info(f"Transformation Successful. Final data loaded into {TARGET_TABLE}.")
        
        # Visualize the final Data Warehouse table
        display_table(TARGET_TABLE, conn, False, f"DATA WAREHOUSE: {TARGET_TABLE}")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Transformation Failed: {e}", exc_info=True)
        print(f"\n[FAILURE] Check logs for details. Transformation failed.")
        return False

# -----------------------------------------------------------------
## 3. ORCHESTRATION & MONITORING 
# -----------------------------------------------------------------

def run_full_pipeline_scheduled():
    """Defines the dependency and runs the entire pipeline with logging."""
    print("\n" + "#"*60)
    logging.info("<<< ORCHESTRATOR: KICKING OFF FULL ELT PIPELINE RUN >>>")
    start_time = time.time()
    
    # Task 1: E & L (Dependency Check: If E&L fails, we stop)
    if not extract_and_load_raw_data():
        logging.error("PIPELINE HALTED at E & L stage.")
        return
        
    # Task 2: T & Final Load (Dependency Check: If T fails, we stop)
    if not transform_and_load_dimensions():
        logging.error("PIPELINE HALTED at Transformation stage.")
        return
        
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logging.info(f"ELT PIPELINE COMPLETE AND SUCCESSFUL. Duration: {duration} seconds.")
    print("#"*60 + "\n")


def schedule_pipeline_run():
    """Sets up a real scheduler (Cron/Airflow replica)."""
    
    # Schedule every 10 seconds for a quick demo
    schedule.every(10).seconds.do(run_full_pipeline_scheduled)
    
    print("\n--- SCHEDULER INITIALIZED (Airflow Replica) ---")
    print(f"Pipeline is scheduled to run every 10 seconds.")
    print("Watching for scheduled tasks... (Press Ctrl+C to stop)")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

# -----------------------------------------------------------------
## 4. CLI INTERFACE
# -----------------------------------------------------------------

def main_cli():
    """The main command-line interface."""
    
    # Initial setup check
    if not os.path.exists(DB_NAME):
        print(f"[SETUP] Database file '{DB_NAME}' not found. Initializing...")
        
    while True:
        print("\n" + "="*60)
        print("ELT PIPELINE VISUALIZER CLI")
        print("="*60)
        print("1. Run E & L Phase (Extract Raw Data -> Staging)")
        print("2. Run T Phase (Staging -> Transform -> Final Warehouse)")
        print("3. Run Full Pipeline (E -> L -> T)")
        print("4. **SCHEDULE** Full Pipeline (Watch the logs for automation)")
        print("5. View Staging Data (raw_customers_staging)")
        print("6. View Final Data (dim_customers_final)")
        print("7. Exit")
        
        choice = input("Enter choice (1-7): ")
        
        if choice == '1':
            extract_and_load_raw_data()
        elif choice == '2':
            transform_and_load_dimensions()
        elif choice == '3':
            run_full_pipeline_scheduled()
        elif choice == '4':
            try:
                schedule_pipeline_run()
            except KeyboardInterrupt:
                print("\nScheduler stopped by user.")
        elif choice == '5':
            display_table(SOURCE_TABLE)
        elif choice == '6':
            display_table(TARGET_TABLE)
        elif choice == '7':
            print("Exiting CLI. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main_cli()