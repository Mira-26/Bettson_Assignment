# src/modeling/build_schema.py

import sys
# TODO: replace prints with structured logging
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.config import load_config

"""
Script to initialize the star-schema directory structure
and defines empty Parquet files with explicit schemas for dimensions and
fact tables in the dedicatede file directory. This is suppose to replicate 
the traditional DDL operations
"""

# Mapping from string to pa.DataType
PA_TYPES = {
    'int32': pa.int32(),
    'int16': pa.int16(),
    'int8': pa.int8(),
    'float64': pa.float64(),
    'string': pa.string(),
    'timestamp[s]': pa.timestamp('s')
}


def build_schemas_from_config(schema_conf: dict) -> dict:
    """
    Build a dict of schemas from config definitions as we need to map string type of the config file to Python types
    Args:
        schema_conf: Mapping of table names to list of field definitions
    Returns:
        Dict[str, pa.Schema]
    """
    schemas = {}
    for table, fields in schema_conf.items():
        pa_fields = []
        for field_def in fields:
            name = field_def['name']
            dtype = field_def['type']
            pa_type = PA_TYPES.get(dtype)
            if pa_type is None:
                print(f"Unknown type '{dtype}' for field '{name}' in '{table}'")
                sys.exit(1)
            pa_fields.append(pa.field(name, pa_type))
        schemas[table] = pa.schema(pa_fields)
    return schemas


def main():
    # Load configuration
    cfg = load_config()
    base = Path(cfg['paths']['model_dest_base'])
    schema_conf = cfg.get('schemas', {})

    if not schema_conf:
        print("No 'schemas' section found in config/config.yaml")  #TODO: replace print statements with appropriate logging
        sys.exit(1)

    # Build schemas
    SCHEMAS = build_schemas_from_config(schema_conf)

    try:
        base.mkdir(parents=True, exist_ok=True)
        print(f"Model base directory at {base}")

        # Creation of subdirectories and empty schema files
        for table, schema in SCHEMAS.items():
            table_dir = base / table
            table_dir.mkdir(exist_ok=True)
            print(f"Created directory: {table_dir}")

            # Write the defined schema
            empty_table = pa.Table.from_batches([], schema)
            parquet_path = table_dir / f"{table}.parquet"
            pq.write_table(empty_table, parquet_path)
            print(f"Initialized schema file: {parquet_path}")

    except Exception as e:
        print(f"Error building schema structure: {e}")
        sys.exit(1)

    print("Schema directories and schema files initialized successfully.")

if __name__ == "__main__":
    main()
