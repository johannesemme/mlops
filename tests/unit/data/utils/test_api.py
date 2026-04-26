from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data.utils.api import fetch_records_as_pdf

_URL = "https://api.example.com/dataset"


def test_returns_dataframe(requests_mock):
    records = [{"TimeUTC": "2024-01-01", "PriceArea": "DK1"}]
    requests_mock.get(_URL, json={"records": records})
    df = fetch_records_as_pdf(_URL, params={})
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_empty_response_returns_empty_dataframe(requests_mock):
    requests_mock.get(_URL, json={"records": []})
    df = fetch_records_as_pdf(_URL, params={})
    assert df.empty


def test_pagination_fetches_all_pages(requests_mock):
    page1 = [{"TimeUTC": f"2024-01-01 0{i}:00", "PriceArea": "DK1"} for i in range(3)]
    page2 = [{"TimeUTC": "2024-01-01 04:00", "PriceArea": "DK1"}]

    responses = [
        {"json": {"records": page1}},
        {"json": {"records": page2}},
    ]
    requests_mock.get(_URL, responses)

    df = fetch_records_as_pdf(_URL, params={}, limit=3)
    assert len(df) == 4


def test_http_error_raises(requests_mock):
    # If the status_code is 500, the function should raise an HTTPError
    requests_mock.get(_URL, status_code=500)
    with pytest.raises(Exception):
        fetch_records_as_pdf(_URL, params={})


def test_missing_records_key_raises_value_error(requests_mock):
    requests_mock.get(_URL, json={"error": "rate limit exceeded"})
    with pytest.raises(ValueError, match="no 'records' key"):
        fetch_records_as_pdf(_URL, params={})
