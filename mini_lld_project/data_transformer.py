import re
import numpy as np
import pandas as pd
from tabulate import tabulate

def _visualize_transformation(df, step_name):
    """Helper to visualize the result of a transformation."""
    print(f"\n--- TRANSFORMATION STEP: {step_name} ---")
    print(f"Records after operation: {len(df)}")
    if len(df) == 0:
        print("[INFO] No rows to display.")
        return
    print(tabulate(df.head(10), headers='keys', tablefmt='fancy_grid', showindex=False))

def _dtype_buckets(df):
    """Return columns grouped by broad dtype families."""
    str_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    
    def _dedupe(seq):
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out
        
    num_cols = df.select_dtypes(include=["number", "float", "int"]).columns.tolist()
    dt_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    cat_cols = df.select_dtypes(include=["category"]).columns.tolist()
    
    return _dedupe(str_cols), _dedupe(num_cols), _dedupe(dt_cols), _dedupe(cat_cols)

def _enumerate_options(options):
    return [f"{i+1}. {opt}" for i, opt in enumerate(options)]

def _parse_selection(user_inp, options, allow_multi=True):
    """Parses user input for selection (index or name)."""
    if not user_inp:
        return []
    
    chosen, seen = [], set()
    name_map = {str(o).lower(): o for o in options}
    tokens = [t.strip() for t in user_inp.split(",") if t.strip()]
    
    for tok in tokens:
        candidate = None
        
        if tok.isdigit():
            idx = int(tok) - 1
            if 0 <= idx < len(options):
                candidate = options[idx]
        else:
            key = tok.lower()
            if key in name_map:
                candidate = name_map[key]
            else:
                found = next((o for o in options if str(o).lower() == key), None)
                if found:
                    candidate = found
                else:
                    print(f"[WARN] Column/Option '{tok}' not found. Skipping.")
        
        if candidate is not None and candidate not in seen:
            chosen.append(candidate)
            seen.add(candidate)
            
    if allow_multi:
        return chosen
    return chosen[:1] if chosen else []

def _prompt_select_columns(df, prompt, candidates, allow_multi=True, required=True):
    """Prompt user to select columns from given candidates; retries to avoid invalids."""
    if not candidates:
        print("[ERROR] No valid columns available for this operation. Skipping.")
        return []
    
    print(prompt)
    print("\n".join(_enumerate_options(candidates)))
    print("(Enter by number, name, or comma-separated list. Type 'skip' to cancel.)")
    
    while True:
        ans = input("> ").strip()
        if ans.lower() == "skip":
            return []
            
        selected = _parse_selection(ans, candidates, allow_multi=allow_multi)
        
        if selected:
            return selected
            
        if not required and not ans: 
            return []
            
        print("[INFO] Please choose from the listed options. Try again or type 'skip'.")

def _show_unique_values(df, col, max_values=20):
    """Show a sample of unique values to guide selection."""
    if col not in df.columns:
        return
    vals = df[col].dropna().astype(str)
    vc = vals.value_counts()
    distinct = len(vc)
    print(f"[INFO] Column '{col}' has {distinct} distinct non-null values.")
    to_show = vc.head(max_values)
    if len(vc) > max_values:
        print(f"[INFO] Showing top {max_values} values by frequency:")
    else:
        print("[INFO] Showing all values:")
    print(tabulate(to_show.reset_index().rename(columns={"index": col, col: "count"}), headers='keys', tablefmt='fancy_grid', showindex=False))

def _flatten_agg_columns(df):
    """Flatten MultiIndex columns from groupby.agg into 'col_func' style."""
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for levels in df.columns.values:
            parts = [str(p) for p in levels if p != ""]
            new_col_name = "_".join(parts).strip("_")
            new_cols.append(new_col_name)
        df.columns = new_cols
    return df

def clean_names(df, automated=False):
    """Standardizes text columns (strips whitespace, normalizes, applies casing)."""
    print("\n--- CLEANING TEXT COLUMNS ---")
    str_cols, _, _, _ = _dtype_buckets(df)
    
    if not str_cols:
        print("[INFO] No string-like columns found to clean. Skipping.")
        return df

    if automated:
        selected = ["name"] if "name" in df.columns else str_cols[:1]
        casing = "title"
        if not selected:
            print("[INFO] Automated run: No suitable columns for default cleaning. Skipping.")
            return df
        print(f"[INFO] Automated run: Cleaning {selected} with default Title Case.")
    else:
        selected = _prompt_select_columns(
            df, 
            "Select one or more string columns to clean:", 
            str_cols, 
            allow_multi=True, 
            required=False
        )

        if not selected:
            print("[INFO] No columns selected. Skipping cleaning.")
            return df
            
        print("\nChoose casing for selected columns:")
        print("  1. Title Case (Default)")
        print("  2. lower case")
        print("  3. UPPER CASE")
        casing_inp = input("Enter choice (1-3) or press Enter for Title Case: ").strip().lower()
        casing = {
            "1": "title",
            "2": "lower",
            "3": "upper",
        }.get(casing_inp, "title") 

    def _apply_casing(series, mode):
        non_null_mask = series.notna()
        series_str = series.loc[non_null_mask].astype(str)
        
        if mode == "title":
            series.loc[non_null_mask] = series_str.str.title()
        elif mode == "lower":
            series.loc[non_null_mask] = series_str.str.lower()
        elif mode == "upper":
            series.loc[non_null_mask] = series_str.str.upper()
        return series

    df_copy = df.copy()
    for col in selected:
        if col not in df_copy.columns:
            continue
        
        df_copy[col] = (
            df_copy[col]
            .astype("string")
            .str.normalize("NFKC")
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        df_copy[col] = _apply_casing(df_copy[col], casing)

    _visualize_transformation(df_copy, f"Cleaned columns {selected} (casing={casing})")
    return df_copy

def filter_data(df):
    """Filters the DataFrame based on user-specified column and value."""
    print("\n--- FILTERING DATA ---")
    all_cols = df.columns.tolist()
    
    col_list = _prompt_select_columns(df, "Select a column to filter on:", all_cols, allow_multi=False, required=True)
    if not col_list:
        print("[INFO] No column selected. Skipping filter.")
        return df
    col = col_list[0]

    _show_unique_values(df, col, max_values=20)

    value = input(f"Enter exact value to filter for in column '{col}': ").strip()
    if value == "":
        print("[INFO] Empty input. Skipping filter.")
        return df

    df_filtered = df.copy() 
    
    try:
        if pd.api.types.is_numeric_dtype(df_filtered[col]):
            val = float(value)
            df_filtered = df_filtered[df_filtered[col] == val]
        else:
            standardized_value = value.lower()
            mask = (
                df_filtered[col]
                .astype(str)
                .fillna('') 
                .str.strip()
                .str.lower() 
                == standardized_value
            )
            df_filtered = df_filtered[mask]
        print(f"[INFO] Filtering on: {col} == '{value}' (Case-Insensitive Match)")
        if df_filtered.empty:
            print("[WARN] Filter resulted in an **EMPTY** DataFrame (0 rows).")
        _visualize_transformation(df_filtered, f"Filtered Data (where {col} = {value})")
        return df_filtered 
    except ValueError:
        print(f"[ERROR] Could not compare input '{value}' with values in column '{col}'. Skipping filter.")
        return df
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during filtering: {e}. Skipping filter.")
        return df

def aggregate_data(df):
    """Performs GROUP BY and aggregation."""
    print("\n--- AGGREGATING DATA (GROUP BY) ---")
    str_cols, num_cols, dt_cols, cat_cols = _dtype_buckets(df)

    likely_groupers = list(dict.fromkeys(cat_cols + str_cols)) or df.columns.tolist()
    group_cols = _prompt_select_columns(
        df,
        "Select one or more GROUP BY columns:",
        likely_groupers,
        allow_multi=True,
        required=True
    )
    if not group_cols:
        print("[INFO] No group-by columns selected. Skipping aggregation.")
        return df

    ordered_candidates = list(dict.fromkeys(num_cols + dt_cols + str_cols))
    agg_cols = _prompt_select_columns(
        df,
        "Select one or more columns to aggregate:",
        ordered_candidates,
        allow_multi=True,
        required=True
    )
    if not agg_cols:
        print("[INFO] No aggregation columns selected. Skipping aggregation.")
        return df

    print("\nChoose aggregation functions per column. You can enter multiple (e.g., 'sum,mean').")
    agg_map = {}

    def _mode_series(x):
        m = x.dropna().mode()
        return m.iat[0] if not m.empty else np.nan
    _mode_series.__name__ = "mode"

    for col in agg_cols:
        if col in num_cols:
            valid_funcs = ["sum", "mean", "median", "min", "max", "count", "nunique"]
        elif col in dt_cols:
            valid_funcs = ["min", "max", "count", "nunique"]
        else:
            valid_funcs = ["count", "nunique", "mode"]

        print(f"\nColumn: {col}")
        print("Available functions:", ", ".join(valid_funcs))
        funcs_inp = input("> ").strip().lower()
        funcs = [f.strip() for f in funcs_inp.split(",") if f.strip()] if funcs_inp else []

        if not funcs:
            funcs = ["count"] if col not in num_cols else ["sum"]
            print(f"[INFO] Using default aggregation for '{col}': {funcs}")

        agg_fns = []
        for f in funcs:
            if f in valid_funcs:
                agg_fns.append(_mode_series if f == "mode" else f)
            else:
                print(f"[WARN] Function '{f}' is not valid for column '{col}'. Skipping.")
                
        if not agg_fns:
            print(f"[ERROR] No valid functions chosen for '{col}'. Skipping this column.")
            continue

        agg_map[col] = agg_fns

    if not agg_map:
        print("[INFO] No valid aggregation specs. Skipping aggregation.")
        return df

    try:
        agg_result = df.groupby(group_cols, dropna=False).agg(agg_map).reset_index()
        agg_result = _flatten_agg_columns(agg_result)
        _visualize_transformation(agg_result, f"Aggregated by {group_cols}")
        return agg_result
    except Exception as e:
        print(f"[ERROR] Aggregation failed: {e}. Skipping.")
        return df