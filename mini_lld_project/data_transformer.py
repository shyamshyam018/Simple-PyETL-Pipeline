# data_transformer.py
import pandas as pd
from tabulate import tabulate

def _visualize_transformation(df, step_name):
    """Helper to visualize the result of a transformation."""
    print(f"\n--- TRANSFORMATION STEP: {step_name} ---")
    print(f"Records after operation: {len(df)}")
    print(tabulate(df.head(), headers='keys', tablefmt='fancy_grid', showindex=False))
    
def clean_names(df):
    """Standardizes string columns (e.g., names) to Title Case."""
    if 'name' in df.columns:
        df['name'] = df['name'].str.title()
        _visualize_transformation(df, "Standardized 'name' to Title Case")
    return df

def filter_data(df):
    """Filters the DataFrame based on user-specified column and value."""
    print("\n--- FILTERING DATA ---")
    
    # 1. Get Column
    print("Available columns:", df.columns.tolist())
    col = input("Enter column to filter on: ").strip()
    if col not in df.columns:
        print(f"[ERROR] Column '{col}' not found. Skipping filter.")
        return df

    # 2. Get Value
    value = input(f"Enter value to filter in column '{col}' (e.g., 'Consumer' or 'California'): ").strip()
    
    # Simple check for numeric vs. string filter
    if pd.api.types.is_numeric_dtype(df[col]):
        try:
            val = float(value)
            df_filtered = df[df[col] == val]
        except ValueError:
            print("[ERROR] Invalid numeric value. Skipping filter.")
            return df
    else:
        df_filtered = df[df[col].astype(str).str.strip() == value]
        
    print(f"[INFO] Filtering on: {col} == '{value}'")
    _visualize_transformation(df_filtered, f"Filtered Data (where {col} = {value})")
    return df_filtered

def aggregate_data(df):
    """Performs a GROUP BY and aggregation based on user input."""
    print("\n--- AGGREGATING DATA (GROUP BY) ---")
    
    # 1. Get Grouping Column
    print("Available columns:", df.columns.tolist())
    group_col = input("Enter column to GROUP BY (e.g., 'segment'): ").strip()
    if group_col not in df.columns:
        print(f"[ERROR] Group column '{group_col}' not found. Skipping aggregation.")
        return df

    # 2. Get Aggregation Column and Method (Simplified)
    print("Enter numeric column and aggregation method (sum, count, mean).")
    agg_col = input("Enter column to aggregate (e.g., 'amount'): ").strip()
    agg_method = input("Enter aggregation method (sum/count/mean): ").strip().lower()
    
    if agg_col not in df.columns or agg_method not in ['sum', 'count', 'mean']:
        print("[ERROR] Invalid aggregation column or method. Skipping aggregation.")
        return df
        
    # Perform aggregation
    try:
        agg_result = df.groupby(group_col).agg({agg_col: agg_method}).reset_index()
        agg_result.rename(columns={agg_col: f'{agg_method}_{agg_col}'}, inplace=True)
        _visualize_transformation(agg_result, f"Aggregated by {group_col} ({agg_method} of {agg_col})")
        return agg_result
    except Exception as e:
        print(f"[ERROR] Aggregation failed: {e}. Skipping.")
        return df