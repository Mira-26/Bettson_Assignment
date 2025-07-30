# Betsson-July-2025
Assessment project for Betsson - Senior Data Engineer position 


Initial Setup

        git clone <repo_url>
        cd codebase


Install Dependencies

        pip install -r requirements.txt

Project Structure

    Bettson_Assignment/            # Project root
    ├── config/                    # Configuration files
    │   └── config.yaml            # Paths, parameters, and other settings
    ├── utils/
        └── config.py              # Support methods
    ├── data/                      # Data storage
    │   ├── source/                # Input data
    ├── requirements.txt           # Python dependencies
    ├── src/
    │   ├── orchestration
    │       └── run_pipeline.py    # Orchestrator: runs the solution end-to-end
    │   ├── ingestion/             
    │   │   └── ingest.py          # Bronze layer: raw ingestion logic
    │   ├── transformation/     
    │   │   └── transform.py       # Silver layer: data cleaning and Quality Checks
    │   ├── reporting/             
    │       └── populate_dw.py     # Gold layer: dimension + fact builders
    │       └── build_schema.py    # Schema creation for star schema folders
    ├   └──tests/                 
    │       └── tests.py           # Unit tests 
    │──README.MD                   # Describes the project
    │──requirements.txt            # lists all required dependencies
    │──.gitignore                  # excludes files from git





Program execution

    You can execute the whole pipeline end-to-end with the following command:
        python run_pipeline.py

    You can also invoke each stage individually:

        python -m src.ingestion.ingest
        python -m src.transformation.transform
        python -m src.reporting.build_schema
        python -m src.reporting.populate_dw

    You can execute the unit test with the following command:

        pytest src/tests/tests.py src/tests/tests.py

Access Stored Results

        After a successful run, your data will be available under data/destination


License

        This project is licensed under the MIT License.