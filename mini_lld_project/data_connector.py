import os
import pandas as pd
import logging
import sqlite3 

def load_source_data(file_path):
    """Loads data from a source file (CSV, TXT, or SQLite)."""
    logging.info(f"Connecting to source: {file_path}")
    if not os.path.exists(file_path):
        logging.error(f"Source file not found: {file_path}")
        return pd.DataFrame()
        
    try:
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.db' or file_ext == '.sqlite':
            conn = sqlite3.connect(file_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            if not tables:
                logging.error("SQLite Database is empty: No tables found.")
                conn.close()
                return pd.DataFrame()
            print("\nAvailable Tables in SQLite Database:")
            for i, t in enumerate(tables):
                print(f" {i+1}. {t}")
            
            table_name = ""
            while table_name not in tables:
                choice_inp = input("Enter table name or number to load: ").strip()
                if choice_inp.isdigit():
                    idx = int(choice_inp) - 1
                    if 0 <= idx < len(tables):
                        table_name = tables[idx]
                    else:
                        print("Invalid number. Try again.")
                elif choice_inp in tables:
                    table_name = choice_inp
                else:
                    print(f"Table '{choice_inp}' not found in the database. Try again.")

            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn) 
            conn.close()
            logging.info(f"Loaded {len(df)} records from table: {table_name}")
            return df
            
        elif file_ext in ('.csv', '.txt'):
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin1') 
            return df
            
        else:
            logging.error(f"Unsupported file type: {file_ext}. Only CSV, TXT, and SQLite are supported.")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        return pd.DataFrame()
    
def save_staging_data(df, stage_name):
    """Saves intermediate data to the staging area."""
    os.makedirs('staging_data', exist_ok=True)
    file_path = os.path.join('staging_data', f"{stage_name}.csv")
    logging.info(f"Saving {len(df)} records to STAGING: {file_path}")
    df.to_csv(file_path, index=False)

def save_final_data(df, final_name):
    """Saves the final transformed data to the warehouse area."""
    os.makedirs('data_warehouse', exist_ok=True)
    file_path = os.path.join('data_warehouse', f"{final_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
    logging.info(f"Saving {len(df)} records to FINAL WAREHOUSE: {file_path}")
    df.to_csv(file_path, index=False)