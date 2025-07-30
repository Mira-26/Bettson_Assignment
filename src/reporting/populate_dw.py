# src/modeling/populate_dw.py

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from src.utils.config import load_config

# TODO: replace prints with structured logging
# TODO: implement SCD Type 2 logic in dimension builders if dataset is loaded incrementally (e.g., add EffectiveDate/EndDate columns and handle updates)


def build_date_dim(silver_data: pd.DataFrame) -> pd.DataFrame:
    """
    Builds Date dimension:
    Generates all dates from 2000-01-01 to 2025-01-01,
    plus a default row for missing dates (1999-01-01).
    Assigns a surrogate key to each date record and enforces uniqueness.
    """
    start_date = pd.to_datetime('2000-01-01')
    end_date   = pd.to_datetime('2025-01-01')
    all_dates  = pd.date_range(start=start_date, end=end_date)
    df_date    = pd.DataFrame({'InvoiceDate': all_dates})

    default_date = pd.to_datetime('1999-01-01')
    df_date = pd.concat([
        pd.DataFrame({'InvoiceDate': [default_date]}),
        df_date
    ], ignore_index=True)

    df_date = df_date.sort_values('InvoiceDate').reset_index(drop=True)
    df_date['DateKey'] = df_date.index + 1

    if not df_date['DateKey'].is_unique:
        print("ERROR: Duplicate DateKey detected in Dim_Date")
        sys.exit(1)
    if not df_date['InvoiceDate'].is_unique:
        print("ERROR: Duplicate InvoiceDate in Dim_Date natural key")
        sys.exit(1)

    return df_date[['DateKey', 'InvoiceDate']]


def build_customer_dim(silver_data: pd.DataFrame) -> pd.DataFrame:
    """
    Builds Customer dimension:
    Uses CustomerID and Country, assigns a surrogate key to each distinct customer,
    enforces surrogate key and natural key uniqueness.
    """
    df_cust = silver_data[['Customer ID', 'Country']].copy()
    df_cust = df_cust.rename(columns={'Customer ID': 'CustomerID'})
    df_cust = df_cust.drop_duplicates(subset=['CustomerID']).reset_index(drop=True)
    df_cust['CustomerKey'] = df_cust.index + 1

    if not df_cust['CustomerKey'].is_unique:
        print("ERROR: Duplicate CustomerKey detected in Dim_Customer")
        sys.exit(1)
    if not df_cust['CustomerID'].is_unique:
        print("ERROR: Duplicate CustomerID detected in Dim_Customer natural key")
        sys.exit(1)

    return df_cust[['CustomerKey', 'CustomerID', 'Country']]


def build_product_dim(silver_data: pd.DataFrame) -> pd.DataFrame:
    """
    Builds Product dimension:
    Uses ProductID (StockCode), Description, Price; assigns a surrogate key to each distinct product,
    enforces surrogate key and natural key uniqueness.
    """
    df_prod = silver_data[['StockCode', 'Description', 'Price']].copy()
    df_prod = df_prod.rename(columns={'StockCode': 'ProductID'})
    df_prod = df_prod.drop_duplicates(subset=['ProductID']).reset_index(drop=True)
    df_prod['ProductKey'] = df_prod.index + 1

    if not df_prod['ProductKey'].is_unique:
        print("ERROR: Duplicate ProductKey detected in Dim_Product")
        sys.exit(1)
    if not df_prod['ProductID'].is_unique:
        print("ERROR: Duplicate ProductID detected in Dim_Product natural key")
        sys.exit(1)

    return df_prod[['ProductKey', 'ProductID', 'Description', 'Price']]


def build_fact_invoice(silver_data: pd.DataFrame, model_base: Path) -> pd.DataFrame:
    """
    Builds Fact_Invoice by merging surrogate keys from dimensions,
    enforces referential integrity (no missing foreign keys).
    Includes diagnostic shape and cardinality validation.
    """
    # Load dimension tables
    df_date = pd.read_parquet(model_base / 'Dim_Date' / 'Dim_Date.parquet')
    df_cust = pd.read_parquet(model_base / 'Dim_Customer' / 'Dim_Customer.parquet')
    df_prod = pd.read_parquet(model_base / 'Dim_Product' / 'Dim_Product.parquet')

    # Deduplicate product dimension to ensure unique ProductID
    df_prod = df_prod.drop_duplicates(subset=['ProductID'])

    # Merge Date
    fact = silver_data.merge(
        df_date[['InvoiceDate','DateKey']],
        on='InvoiceDate', how='left', validate='many_to_one'
    )

    # Fill missing DateKey with default surrogate for invalid/missing dates
    default_date_key = df_date.loc[df_date['InvoiceDate']==pd.to_datetime('1999-01-01'),'DateKey'].iloc[0]
    fact['DateKey'] = fact['DateKey'].fillna(default_date_key)

    # Merge Customer
    fact = fact.merge(
        df_cust[['CustomerID','CustomerKey']],
        left_on='Customer ID', right_on='CustomerID',
        how='left', validate='many_to_one'
    )

    # Merge Product
    fact = fact.merge(
        df_prod[['ProductID','ProductKey']],
        left_on='StockCode', right_on='ProductID',
        how='left', validate='many_to_one'
    )

    # Referential integrity checks
    if fact['DateKey'].isna().any():
        print("Missing DateKey in Fact_Invoice; integrity violated")
        sys.exit(1)
    if fact['CustomerKey'].isna().any():
        print("Missing CustomerKey in Fact_Invoice; integrity violated")
        sys.exit(1)
    if fact['ProductKey'].isna().any():
        print("Missing ProductKey in Fact_Invoice; integrity violated")
        sys.exit(1)

    return fact[['DateKey', 'CustomerKey', 'ProductKey', 'Quantity', 'Price']]


def main():
    cfg = load_config()
    model_base = Path(cfg['paths']['model_dest_base'])
    trans_base = Path(cfg['paths']['trans_dest_base'])

    today = datetime.now()
    date_path = f"{today:%Y}/{today:%m}/{today:%d}"
    silver_file = trans_base / date_path / 'invoices_clean.parquet'

    try:
        # Read silver layer data. 
        # For the shake of simplicity  we read from a specific date folder
        # instead of the whole silver parquet files
        silver_data = pd.read_parquet(silver_file)
        print(f"Loaded clean data from {silver_file}")
    except Exception as e:
        print(f"Cannot read clean dataset: {e}")
        sys.exit(1)

    # create a dictionary of Dim_name:Builder_function pairs
    dims = {
        'Dim_Date': build_date_dim,
        'Dim_Customer': build_customer_dim,
        'Dim_Product': build_product_dim
    }
    # for each pair, populate the Dim and write it as parquet
    for name, builder in dims.items():
        model_data_dim = builder(silver_data)
        out_dir = model_base / name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{name}.parquet"
        model_data_dim.to_parquet(out_file, index=False)
        print(f"Wrote {name} with {len(model_data_dim)} rows to {out_file}")

    # create a dictionary of Fact_name:Builder_function pairs
    facts = {
        'Fact_Invoice': build_fact_invoice
    }
    # for each pair, populate the Fact and write it as parquet
    for name, builder in facts.items():
        model_data_fact = builder(silver_data, model_base)
        out_dir = model_base / name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{name}.parquet"
        model_data_fact.to_parquet(out_file, index=False)
        print(f"Wrote {name} with {len(model_data_fact)} rows to {out_file}")

if __name__ == '__main__':
    main()
