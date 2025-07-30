import yaml
from dateutil import parser
import pandas as pd

"""
Support Python file to help with I/O operations, string formatting etc.
"""

"""
parses a YAML configuration file
Args:
    path: Relative or absolute path to the YAML config file
Returns:
    the YAML configuration
"""
def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)
    
"""
parses a date into a pandas Timestamp
Args:
    value: The input value to parse; expected to be a string or datetime
Returns:
    A pandas.Timestamp

"""
def safe_parse_date(input_date):
        try:
            return parser.parse(input_date) if isinstance(input_date, str) else input_date
        except Exception:
            return pd.NaT
