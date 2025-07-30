from ..utils.config import load_config
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

def main():
    # load config parameters 
    cfg = load_config()
    raw_base = Path(cfg["paths"]["raw_dest_base"])
    enc = cfg["parameters"]["encoding"]

    try:

        # read input data
        input_data = pd.read_csv(cfg["paths"]["raw_csv"],
            encoding=enc, # input data isnt UTF‑8 encoded, TODO: pass encoding in config file
            dtype=str)

    except FileNotFoundError as e:
        print(f"ERROR: Cannot find file – {e}", file=sys.stderr) #TODO: replace print statements with logging
        sys.exit(1)
    except UnicodeDecodeError as e:
        print(f"ERROR: Unicode error with encoding '{enc}': {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error reading CSV: {e}", file=sys.stderr)
        sys.exit(1)


    today = datetime.now()
    dest_path = raw_base / f"{today:%Y}" / f"{today:%m}" / f"{today:%d}" / "invoices.parquet"
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        input_data.to_parquet(dest_path, index=False)
        print(f"Raw data written to {dest_path}")
    except Exception as e:
        print(f"Error writing Parquet: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()        