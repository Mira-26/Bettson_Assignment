# pytest src/tests/tests.py src/tests/tests.py

import pandas as pd
import pytest
from pathlib import Path

from src.reporting.populate_dw import build_fact_invoice

# Helper DataFrames for dimensions
DIM_DATE = pd.DataFrame({
    'InvoiceDate': pd.to_datetime(['1999-01-01', '2025-07-28']),
    'DateKey': [1, 2]
})
DIM_CUST = pd.DataFrame({
    'CustomerKey': [1],
    'CustomerID': ['C001'],
    'Country': ['US']
})
DIM_PROD = pd.DataFrame({
    'ProductKey': [1],
    'ProductID': ['P100'],
    'Description': ['Widget'],
    'Price': [2.0]
})

@pytest.fixture(autouse=True)
def mock_read_parquet(monkeypatch):
    """
    Mock read_parquet to return prebuilt dataFrames
    based on the file path string
    """
    def fake_read_parquet(path, *args, **kwargs):
        path_str = str(path)
        if 'Dim_Date' in path_str:
            return DIM_DATE
        if 'Dim_Customer' in path_str:
            return DIM_CUST
        if 'Dim_Product' in path_str:
            return DIM_PROD
        raise FileNotFoundError(f"Unexpected path: {path_str}")

    monkeypatch.setattr(pd, 'read_parquet', fake_read_parquet)
    yield


def test_build_fact_invoice_success():
    """
    Silver data references existing dimension keys => should build a valid fact.
    """
    silver_data = pd.DataFrame({
        'InvoiceDate': pd.to_datetime(['2025-07-28']),
        'Customer ID': ['C001'],
        'StockCode': ['P100'],
        'Quantity': [5],
        'Price': [2.0]
    })
    # Use any dummy model_base since read_parquet is patched
    model_base = Path('/does/not/matter')

    fact = build_fact_invoice(silver_data, model_base)
    assert list(fact.columns) == ['DateKey', 'CustomerKey', 'ProductKey', 'Quantity', 'Price']
    assert fact.shape == (1, 5)
    row = fact.iloc[0]
    assert row['DateKey'] == 2
    assert row['CustomerKey'] == 1
    assert row['ProductKey'] == 1
    assert row['Quantity'] == 5
    assert row['Price'] == 2.0


def test_build_fact_invoice_missing_product(monkeypatch):
    """
    Silver data references non-existent product => should raise SystemExit
    """
    silver_data = pd.DataFrame({
        'InvoiceDate': pd.to_datetime(['2025-07-28']),
        'Customer ID': ['C001'],
        'StockCode': ['P999'],  # not in DIM_PROD
        'Quantity': [1],
        'Price': [10.0]
    })
    model_base = Path('/does/not/matter')

    with pytest.raises(SystemExit):
        build_fact_invoice(silver_data, model_base)
