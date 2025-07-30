#!/usr/bin/env python3
"""
Simple orchestrator to sequentially execute the 4 pipeline steps:
1) Ingestion
2) Transformation
3) Schema build
4) Data warehouse population
"""
import sys

# Import each step's entry point
from src.ingestion.ingest import main as ingest_main
from src.transformation.transform import main as transform_main
from src.reporting.build_schema import main as build_schema_main
from src.reporting.populate_dw import main as populate_dw_main


def main():
    try:
        print("\n=== STEP 1: Ingestion ===")
        ingest_main()

        print("\n=== STEP 2: Transformation ===")
        transform_main()

        print("\n=== STEP 3: Schema Build ===")
        build_schema_main()

        print("\n=== STEP 4: Populate DW ===")
        populate_dw_main()

        print("\nPipeline completed successfully!")
    except SystemExit as e:
        # Each step may call sys.exit on error
        print(f"Pipeline aborted: {e}")
        sys.exit(e.code)
    except Exception as e:
        print(f"Unexpected error during pipeline execution: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()