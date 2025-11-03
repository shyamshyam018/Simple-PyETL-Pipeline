import pandas as pd
import sqlite3
import os
from datetime import datetime
from tabulate import tabulate

def  _display_df(df,title):
    """Internal helper for consistent visualization."""
    print("\n" + "="*80)
    print(f"| {title.center(76)} |")
    print("="*80)
    if df.empty:
        print("| {'DataFrame is empty.'.center(76)} |")
    else:
        print(tabulate(df.head(), headers='keys', tablefmt='fancy_grid', showindex=False))
        print(f"--- Total Records: {len(df)} ---")

def load_source_data(file_path):
    """Loads data from CSV, TXT, or SQLite based on extension."""
    
    print(f"\n[CONNECTOR] Attempting to load data from: {file_path}")
    df = pd.DataFrame()
    
    try:
        if file_path.endswith('.csv') or file_path.endswith('.txt'):
            # Handling CSV/TXT (assuming CSV format for simplicity)
            df = pd.read_csv(file_path)
            print(f"[SUCCESS] Loaded data from CSV/TXT.")
            
        elif file_path.endswith('.sqlite') or file_path.endswith('.db'):
            # Handling SQLite
            conn = sqlite3.connect(file_path)
            # Prompt the user for the table name inside the SQLite file
            table_name = input("Enter the table name inside the SQLite file: ")
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            conn.close()
            print(f"[SUCCESS] Loaded data from SQLite table: {table_name}.")
            
        else:
            raise ValueError("Unsupported file type. Must be CSV, TXT, or SQLite.")
            
        _display_df(df, f"EXTRACTION VISUAL: Raw Data from {os.path.basename(file_path)}")
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to load data: {e}")
        return None
    
def save_staging_data(df, stage_name="STAGING"):
    """Saves the current DataFrame to a 'staging' CSV file (The Data Lake)."""
    os.makedirs('staging', exist_ok=True)
    stage_path = f'staging/{stage_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    df.to_csv(stage_path, index=False)
    print(f"[LOAD SUCCESS] Data Staged: Saved {len(df)} rows to {stage_path}")
    print("="*80)

def save_final_data(df, final_name="DIMENSION"):
    """Saves the final DataFrame to a 'processed' CSV file (The Data Warehouse)."""
    os.makedirs('processed', exist_ok=True)
    final_path = f'processed/{final_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    df.to_csv(final_path, index=False)
    print(f"\n[LOAD SUCCESS] Final Data Warehouse Load: Saved {len(df)} rows to {final_path}")
    print("="*80)