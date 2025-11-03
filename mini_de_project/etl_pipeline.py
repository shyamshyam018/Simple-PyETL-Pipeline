import pandas as pd 
import sqlite3
import schedule
import time
from datetime import datetime
import logging
import os
from tabulate import tabulate  # Used for clean table printing in the terminal

DB_NAME = 'customer_data.db'
SOURCE_TABLE = 'raw_customers_staging'
TARGET_TABLE = 'dim_customers_final'

logging.basicConfig(level=logging.INFO,format='[%(levelname)s %(asctime)s] - %(message)s')

SAMPLE_DATA = [
    ('CG-12520', 'Claire Gute', 'Consumer', 'Kentucky', 'Henderson'),
    ('DV-13045', 'Darrin Van Huff', 'Corporate', 'California', 'Los Angeles'),
    ('SO-20335', 'Sean O\'Donnell', 'Consumer', 'Florida', 'Fort Lauderdale'),
    ('BH-11710', 'Brosina Hoffman', 'Consumer', 'California', 'Los Angeles'),
    ('SG-19345', 'Samantha Green', 'Corporate', 'California', 'Los Angeles'), # Duplicate for testing
    ('SG-19345', 'Samantha Green', 'Corporate', 'California', 'Los Angeles'),
]
COLUMNS = ['id', 'name', 'segment', 'state', 'city']

def display_table(table_name , conn=None , close_conn=True , title_override=None):
    if conn is None:
        conn = sqlite.connect(DB_NAME)
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        title = title_override if title_override else f"Table: {table_name} ({len(df)} rows)"
        print("\n" + "="*50)
        print(f"| {title.center(46)} |")
        print("="*50)
        if df.empty:
            print(f"| {'Table is empty.'.center(46)} |")
        else:
            print(tabulate(df, headers='keys', tablefmt='fancy_grid', showindex=False))
    except pd.io.sql.DatabaseError:
        print(f"\n[ERROR] Table '{table_name}' does not exist or cannot be read.")
    finally:
        if close_conn:
            conn.close()

def extract_and_load_raw_data():
    logging.info(f"--- Starting E & L Phase at {datetime.now()} ---")
