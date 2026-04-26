import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _session() -> requests.Session:
    session = requests.Session()
    # For each of these status codes, we will retry the request up to 3 times with an exponential backoff (1s, 2s, 4s)
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    # Mount the HTTPAdapter with the retry configuration to both HTTP and HTTPS requests
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def fetch_records_as_pdf(url: str, params: dict, limit: int = 10_000) -> pd.DataFrame:
    session = _session()
    offset = 0
    all_records = []

    while True:
        response = session.get(
            url,
            params={**params, "limit": limit, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        records = data.get("records")
        if records is None:
            raise ValueError(f"Unexpected API response (no 'records' key): {data}")

        if not records:
            break

        all_records.extend(records)

        # Checkl if we've reached the last records based on the number of records returned (below limit)
        if len(records) < limit:
            break

        offset += limit

    return pd.DataFrame(all_records)
