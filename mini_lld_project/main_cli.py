import os
import time
import logging
from datetime import datetime
import schedule
import pandas as pd
from tabulate import tabulate
from data_connector import load_source_data, save_staging_data, save_final_data
from data_transformer import clean_names, filter_data, aggregate_data

global_df = None

logging.basicConfig(level=logging.INFO, format='[ORCHESTRATOR] %(asctime)s - %(levelname)s - %(message)s')

def _display_df(df, title="DATAFRAME PREVIEW"):
    """Pretty-print a quick preview with metadata."""
    print(f"\n--- {title} ---")
    print(f"Shape: {df.shape[0]} rows x {df.shape[1]} cols")
    if df.empty:
        print("[INFO] DataFrame is empty.")
        return
    preview_df = df.head(10)
    print(f"Columns: {list(df.columns)}")
    print(tabulate(preview_df, headers='keys', tablefmt='fancy_grid', showindex=False))


def choose_source_file():
    """Prompts user to select a file from the source_data directory."""
    os.makedirs('source_data', exist_ok=True)
    files = [f for f in os.listdir('source_data') if os.path.isfile(os.path.join('source_data', f))]

    if not files:
        print("\n[SETUP REQUIRED] 'source_data' directory is empty. Please add a CSV, TXT, or SQLite file.")
        return None

    print("\nAvailable Source Files:")
    for i, f in enumerate(files):
        print(f"  {i+1}. {f}")

    while True:
        try:
            choice_inp = input("Enter number of file to load or 'skip': ").strip().lower()
            if choice_inp == 'skip':
                return None
            
            choice = int(choice_inp) - 1
            if 0 <= choice < len(files):
                return os.path.join('source_data', files[choice])
            else:
                print("Invalid choice.")
        except ValueError:
            print("Invalid input. Please enter a number or 'skip'.")


def pipeline_step_extract_load():
    """Step 1: Extract from Source and Load to Global DataFrame."""
    global global_df

    file_path = choose_source_file()
    if not file_path:
        return False

    logging.info(f"Task 1: Starting E&L for {os.path.basename(file_path)}")

    df = load_source_data(file_path)

    if df is None or df.empty:
        logging.error("Task 1 Failed: Data extraction returned empty or failed.")
        global_df = None
        return False

    save_staging_data(df, stage_name=f"raw_load_{os.path.basename(file_path).split('.')[0]}")

    global_df = df
    logging.info(f"Task 1 Completed successfully. Data is now staged and in-memory ({len(global_df)} records).")
    _display_df(global_df, "RAW DATA LOADED (Initial State)")
    return True


def pipeline_step_transform():
    """Step 2: Interactive Transformation of the Global DataFrame."""
    global global_df

    if global_df is None or global_df.empty:
        logging.error("Task 2 Failed: No data loaded. Run E&L first.")
        return False
        
    logging.info("Task 2: Starting Interactive Transformation.")
    df_current = global_df.copy() 
    
    while True:
        print("\n" + "~"*50)
        print("INTERACTIVE TRANSFORMATION MENU (Current Records: " + str(len(df_current)) + ")")
        print("~"*50)
        print("1. Clean/Title Case Names")
        print("2. Filter Data (WHERE clause)")
        print("3. Aggregate Data (GROUP BY)")
        print("4. Finish Transformations (Move to Final Load)")
        print("5. View Current Transformed Data") 
        print("6. Discard changes and Exit Transformation")
        
        choice = input("Enter transformation choice (1-6): ").strip()
        
        df_after_transform = None
        
        if choice == '1':
            df_after_transform = clean_names(df_current)
        elif choice == '2':
            df_after_transform = filter_data(df_current)
        elif choice == '3':
            df_after_transform = aggregate_data(df_current)
        elif choice == '4':
            global_df = df_current 
            logging.info(f"Task 2 Completed successfully. Final records: {len(global_df)}")
            return True
        elif choice == '5':
            _display_df(df_current, "CURRENT STATE (In-Memory Transformation)")
            continue
        elif choice == '6':
            logging.info("Task 2 Aborted. Changes discarded.")
            return False
        else:
            print("Invalid choice.")
            continue

        if df_after_transform is not None and not df_after_transform.equals(df_current):
            df_current = df_after_transform
            print(f"[INFO] Transformation applied. Current record count: {len(df_current)}")
        elif df_after_transform is not None:
             print("[INFO] Transformation executed but resulted in **no change** to the DataFrame. State is unchanged.")
        else:
            print("[WARN] Transformation failed or returned None. State not updated.")


def pipeline_step_final_load():
    """Step 3: Load the transformed data to the final processed folder."""
    global global_df

    if global_df is None or global_df.empty:
        logging.error("Task 3 Failed: No transformed data to load.")
        return False
    
    logging.info(f"Task 3: Starting Final Load ({len(global_df)} records).")
    save_final_data(global_df, final_name="FINAL_ANALYTICS")
    global_df = None
    logging.info("Task 3 Completed successfully. Pipeline finished.")
    return True

def run_full_pipeline_scheduled():
    """Runs the entire pipeline sequentially (used by scheduler)."""

    print("\n" + "#"*70)
    logging.info("--- SCHEDULER TRIGGERED FULL PIPELINE RUN (Assumes 'data.csv' exists) ---")

    default_file = os.path.join('source_data', 'data.csv')
    if not os.path.exists(default_file):
         logging.error(f"Scheduled pipeline skipped. Default file '{default_file}' not found.")
         print("#"*70 + "\n")
         return
    
    global global_df
    df_scheduled = load_source_data(default_file)
    if df_scheduled is None or df_scheduled.empty:
         logging.error("Full Pipeline Halted at Scheduled E&L.")
         print("#"*70 + "\n")
         return
    
    logging.info("Task 2: Starting Scheduled Transformation (Automated).")
    df_scheduled = clean_names(df_scheduled, automated=True) 
    
    if 'name' in df_scheduled.columns:
        df_scheduled = df_scheduled.dropna(subset=['name'])
        logging.info("Scheduled Task: Applied default name cleaning and NaN filtering on 'name' column.")
    
    global_df = df_scheduled 
    
    if not pipeline_step_final_load():
        logging.error("Full Pipeline Halted at Final Load.")
        print("#"*70 + "\n")
        return
        
    logging.info("--- SCHEDULER RUN COMPLETE ---")
    print("#"*70 + "\n")

def schedule_task():
    """Sets up the task scheduler."""
    schedule.every(30).seconds.do(run_full_pipeline_scheduled)
    
    print("\n--- SCHEDULER INITIALIZED (Airflow/Cron Replica) ---")
    print("Full Pipeline is scheduled to run every 30 seconds (Assumes 'source_data/data.csv').")
    print("Press Ctrl+C to stop the scheduler and return to the main menu.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nScheduler stopped by user. Returning to main menu.")

def main_cli():
    """The main command-line interface."""
    global global_df 

    print("\n" + "="*70)
    print("DATA ENGINEERING LLD SIMULATOR (Modularity & Visualization)")
    print("="*70)
    
    while True:
        data_status = f"({len(global_df)} records loaded)" if global_df is not None else "(No data loaded)"

        print("\n--- MAIN MENU ---")
        print(f"Data Status: {data_status}")
        print("1. Run E & L (Extract and Load to Staging)")
        print("2. Run T (Interactive Transformation)")
        print("3. Run L (Final Load to Data Warehouse)")
        print("4. Run Full Pipeline (Scheduled/Automated)")
        print("5. View Current In-Memory Data (global_df)")
        print("6. Exit")
        
        choice = input("Enter choice (1-6): ").strip()
        
        if choice == '1':
            pipeline_step_extract_load()
        elif choice == '2':
            pipeline_step_transform()
        elif choice == '3':
            pipeline_step_final_load()
        elif choice == '4':
            try:
                schedule_task()
            except KeyboardInterrupt:
                print("\nScheduler stopped by user. Returning to main menu.")
        elif choice == '5':
            if global_df is not None and not global_df.empty:
                _display_df(global_df, "CURRENT IN-MEMORY DATAFRAME STATE")
            else:
                print("[INFO] No data loaded into memory yet. Run E&L first.")
        elif choice == '6':
            print("Exiting CLI. Goodbye!")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main_cli()