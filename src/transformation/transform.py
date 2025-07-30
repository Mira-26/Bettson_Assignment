from src.utils.config import load_config, safe_parse_date
from pathlib import Path
from datetime import datetime
import sys
import pandas as pd


def main():

    cfg = load_config()
    raw_base   = Path(cfg["paths"]["raw_dest_base"])
    trans_base = Path(cfg["paths"]["trans_dest_base"])
    pii_base   = Path(cfg["paths"]["pii_dest_base"])
    logs_path = Path(cfg["paths"]["logs_path"])

    today = datetime.now()
    raw_path   = raw_base / f"{today:%Y}/{today:%m}/{today:%d}" / "invoices.parquet"
    trans_path = trans_base / f"{today:%Y}/{today:%m}/{today:%d}" / "invoices_clean.parquet"
    logs_path = pii_base / f"{today:%Y}/{today:%m}/{today:%d}" / raw_base / logs_path / "invoices_qa.parquet"

    # Load raw data
    try:
        raw_dataset = pd.read_parquet(raw_path)
    except FileNotFoundError as e:
        print.error(f"Raw data not found at {raw_path}: {e}") #TODO: replace print statements with appropriate logging
        sys.exit(1)
    except Exception as e:
        print(f"Error reading raw data: {e}")
        sys.exit(1)

    # Check dataset is not empty
    if raw_dataset.empty:
        print("Raw dataset is empty... Exiting program execution.")
        sys.exit(1)

    raw_dataset['InvoiceDate'] = raw_dataset['InvoiceDate'].apply(safe_parse_date)
    bad_dates = raw_dataset['InvoiceDate'].isna()

    if len(bad_dates)>0:
        count = bad_dates.sum()
        print(f"Dropping {count} rows with unparsable InvoiceDate")
        raw_dataset = raw_dataset.dropna(subset=['InvoiceDate'])

    # Check primary key 'Invoice' column
    missing_pk = raw_dataset['Invoice'].isna() | (raw_dataset['Invoice'] == "")
    if len(missing_pk)>0:
        count = missing_pk.sum()
        print(f"Dropping {count} rows with missing Invoice PK")
        raw_dataset = raw_dataset.dropna(subset=['InvoiceDate'])

    # Check for duplicates based on PK
    dups_PK = raw_dataset['Invoice'].duplicated(keep=False) # produces a Series of booleans
    dup_PK_count = dups_PK.sum()
    if dup_PK_count>0:
        print(f"Found {dup_PK_count} duplicate Invoice records")
        # total_rows = len(raw_dataset)
        # print(f"total rows: {total_rows}")

    # Check for pure duplicates
    pure_dups = raw_dataset.duplicated(keep=False)
    # duplicate_rows = raw_dataset[pure_dups]
    # print(f"showing 10 pure dups: {duplicate_rows.head(10)}")
    pure_dups_count = pure_dups.sum()
    if pure_dups_count>0:
        print(f"Found {pure_dups_count} pure duplicated records")
        # drop pure duplicates (all columns)
        raw_dataset = raw_dataset.drop_duplicates(keep='first')
        print(f"Rows after deduplication:  {len(raw_dataset)}")

    # Filter out records where Price or Quantity is <= 0

    # ensure Price and Quantity are numeric (non‑parsable → NaN)
    raw_dataset['Price']    = pd.to_numeric(raw_dataset['Price'],    errors='coerce')
    raw_dataset['Quantity'] = pd.to_numeric(raw_dataset['Quantity'], errors='coerce')

    #  flag rows with ≤0 or missing values
    invalid = (
        (raw_dataset['Price']    <= 0) |
        (raw_dataset['Quantity'] <= 0) |
        raw_dataset['Price'].isna()    |
        raw_dataset['Quantity'].isna()
    )
    count = invalid.sum()
    if count>0:
        print(f"Dropping {count} rows with zero/negative or non‑numeric Price/Quantity")
        raw_dataset = raw_dataset.loc[~invalid]

    # TODO: Add more checks

    # TODO: Separate PII columns if present

    # TODO: Assemble data quality issues and write them to a logs folder for further processing

    # Write transformed (silver) data
    try:
        trans_path.parent.mkdir(parents=True, exist_ok=True)
        raw_dataset.to_parquet(trans_path, index=False)
        print(f"Transformed data written to {trans_path}")
    except Exception as e:
        print(f"Error writing transformed data: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
