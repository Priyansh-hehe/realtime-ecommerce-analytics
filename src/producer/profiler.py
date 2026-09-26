"""
src/producer/profiler.py
------------------------
Dataset Merger & Data Quality Profiler

1. Checks if `data/raw/online_retail.csv` exists; if not (or if --force-merge is passed),
   combines the multi-year raw transaction files into a unified dataset.
2. Profiles the dataset and highlights edge cases (Null Customer IDs, Cancellations,
   Negative Quantities, Zero Prices) to guide Consumer validation rules.
"""

import os
import sys
import glob
import argparse
import pandas as pd

# Ensure UTF-8 output encoding for Windows terminal
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")
OUTPUT_CSV = os.path.join(RAW_DIR, "online_retail.csv")


def find_input_files():
    """Finds available raw CSV or Excel files in data/raw/."""
    csv_files = glob.glob(os.path.join(RAW_DIR, "*.csv"))
    csv_files = [f for f in csv_files if os.path.abspath(f) != os.path.abspath(OUTPUT_CSV)]
    xlsx_files = glob.glob(os.path.join(RAW_DIR, "*.xlsx"))
    return csv_files, xlsx_files


def load_and_unify_data(force_merge: bool = False) -> pd.DataFrame:
    """Loads unified dataset, or merges raw files if not already present."""
    if os.path.exists(OUTPUT_CSV) and not force_merge:
        print(f"[INFO] '{os.path.basename(OUTPUT_CSV)}' already exists. Loading directly...")
        df = pd.read_csv(OUTPUT_CSV, encoding="utf-8")
        df["invoice_date"] = pd.to_datetime(df["invoice_date"])
        return df

    print(f"[INFO] Merging raw files into '{os.path.basename(OUTPUT_CSV)}'...")
    csv_files, xlsx_files = find_input_files()
    dfs = []

    if csv_files:
        print(f"[INFO] Found {len(csv_files)} source CSV file(s) in data/raw/:")
        for f in sorted(csv_files):
            print(f"       Reading {os.path.basename(f)}...")
            try:
                df = pd.read_csv(f, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(f, encoding="ISO-8859-1")
            dfs.append(df)
    elif xlsx_files:
        xlsx_path = xlsx_files[0]
        print(f"[INFO] Reading Excel sheets from {os.path.basename(xlsx_path)}...")
        xl = pd.ExcelFile(xlsx_path)
        for sheet in xl.sheet_names:
            print(f"       Reading sheet '{sheet}'...")
            df = xl.parse(sheet)
            dfs.append(df)
    else:
        raise FileNotFoundError("No raw CSV or XLSX files found in data/raw/")

    combined_df = pd.concat(dfs, ignore_index=True)

    # Standardize column names to lower_snake_case
    col_map = {
        "Invoice": "invoice_no",
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "Price": "unit_price",
        "UnitPrice": "unit_price",
        "Customer ID": "customer_id",
        "CustomerID": "customer_id",
        "Country": "country"
    }
    combined_df = combined_df.rename(columns=col_map)

    # Ensure invoice_date is datetime and sort chronologically
    combined_df["invoice_date"] = pd.to_datetime(combined_df["invoice_date"])
    combined_df = combined_df.sort_values(by="invoice_date").reset_index(drop=True)

    # Save to unified CSV
    print(f"\n[INFO] Saving unified dataset to {os.path.basename(OUTPUT_CSV)}...")
    combined_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"[INFO] Successfully saved {len(combined_df):,} rows to {OUTPUT_CSV}.")
    return combined_df


def profile_dataset(df: pd.DataFrame):
    """Profiles the dataset and prints a structured analysis."""
    total_rows = len(df)
    print("\n" + "="*65)
    print("       REAL-WORLD DATASET PROFILING REPORT")
    print("="*65)

    # 1. Basic Dimensions
    print("\n1. Overview & Dimensions:")
    print(f"   * Total Rows: {total_rows:,}")
    print(f"   * Columns: {list(df.columns)}")
    print(f"   * Date Range: {df['invoice_date'].min()} to {df['invoice_date'].max()}")
    print(f"   * Unique Invoices: {df['invoice_no'].nunique():,}")
    print(f"   * Unique Products (StockCodes): {df['stock_code'].nunique():,}")
    print(f"   * Unique Customers: {df['customer_id'].dropna().nunique():,}")
    print(f"   * Unique Countries: {df['country'].nunique():,}")

    # 2. Null / Missing Values
    print("\n2. Missing / Null Values:")
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        pct = (count / total_rows) * 100
        status = "WARNING" if pct > 0 else "OK"
        print(f"   * {col:<15}: {count:>8,} missing ({pct:>5.1f}%) [{status}]")

    # 3. Data Integrity & Dirty Data Analysis
    cancellations = df[df["invoice_no"].astype(str).str.startswith("C", na=False)]
    negative_qty = df[df["quantity"] < 0]
    zero_price = df[df["unit_price"] == 0]
    negative_price = df[df["unit_price"] < 0]
    guest_checkout = df[df["customer_id"].isnull()]

    print("\n3. Data Quality & Streaming Edge Cases:")
    print(f"   * Cancellation Invoices (starts with 'C'): {len(cancellations):,} ({len(cancellations)/total_rows*100:.2f}%)")
    print(f"   * Negative Quantities:                     {len(negative_qty):,} ({len(negative_qty)/total_rows*100:.2f}%)")
    print(f"   * Zero Unit Price ($0.00):                  {len(zero_price):,} ({len(zero_price)/total_rows*100:.2f}%)")
    print(f"   * Negative Unit Price (< $0.00):            {len(negative_price):,} ({len(negative_price)/total_rows*100:.2f}%)")
    print(f"   * Guest / Unregistered Checkout (No Cust ID):{len(guest_checkout):,} ({len(guest_checkout)/total_rows*100:.2f}%)")

    # 4. Top Countries
    print("\n4. Top 5 Countries by Volume:")
    top_countries = df["country"].value_counts().head(5)
    for country, count in top_countries.items():
        print(f"   * {country:<20}: {count:>8,} records ({count/total_rows*100:>5.1f}%)")

    print("\n" + "="*65)
    print("  DATA PIPELINE TAKEAWAYS FOR OUR CONSUMER & POSTGRES:")
    print("   1. Missing Customer ID: Must handle anonymous/guest checkouts.")
    print("   2. Negative Quantities/Cancellations: Track returns/refunds vs sales.")
    print("   3. $0.00 or Negative Unit Price: Identify administrative/manual adjustments.")
    print("="*65 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dataset merger and profiler")
    parser.add_argument("--force-merge", action="store_true", help="Force re-merging raw source files even if online_retail.csv exists")
    args = parser.parse_args()

    df = load_and_unify_data(force_merge=args.force_merge)
    profile_dataset(df)
