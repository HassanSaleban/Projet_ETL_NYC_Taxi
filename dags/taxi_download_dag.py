# dags/taxi_bulk_download_dag.py
import os
import time
import json
from typing import Optional, Dict, Iterable, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyarrow.parquet import ParquetFile

from airflow.decorators import dag, task
from datetime import datetime

# -------- Réglages ----------
BASE_FOLDER = "/opt/airflow/exports/staging"     # <-- dossier dans le bind mount existant
COLORS: Iterable[str] = ["yellow"]
YEAR_RANGE: Tuple[int, int] = (2009, 2025)       # intervalle inclusif
MONTHS: Iterable[int] = range(1, 13)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DEFAULT_TIMEOUT = (5, 30)
MAX_RETRIES = 5
BACKOFF = 1.2
SLEEP_BETWEEN = 1.0
SLEEP_BETWEEN_FILES = 1.0
USER_AGENT = "TaxiDataFetcher/2.1 (+Airflow)"
# ----------------------------

# -------- Fonctions utilitaires ----------
def make_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES,
        backoff_factor=BACKOFF, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["HEAD", "GET"]), respect_retry_after_header=True,
    )
    a = HTTPAdapter(max_retries=r)
    s.mount("https://", a)
    s.mount("http://", a)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def ensure_folder(base: str, color: str, year: int) -> str:
    path = os.path.join(base, color, str(year))
    os.makedirs(path, exist_ok=True)
    return path

def head_info(session: requests.Session, url: str) -> Dict[str, Optional[str]]:
    resp = session.head(url, allow_redirects=True, timeout=DEFAULT_TIMEOUT)
    if resp.status_code >= 400:
        raise requests.HTTPError(f"HEAD {resp.status_code} {url}")
    return {
        "status_code": resp.status_code,
        "content_length": resp.headers.get("Content-Length"),
        "last_modified": resp.headers.get("Last-Modified"),
        "etag": resp.headers.get("ETag"),
        "content_type": resp.headers.get("Content-Type"),
    }

def download_stream(session: requests.Session, url: str, out_path: str, expected_size: Optional[int]) -> None:
    time.sleep(SLEEP_BETWEEN)
    with session.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as resp:
        resp.raise_for_status()
        tmp = out_path + ".part"
        written = 0
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MiB
                if chunk:
                    f.write(chunk)
                    written += len(chunk)
        if expected_size is not None and written != expected_size:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise IOError(f"Taille téléchargée {written} ≠ attendue {expected_size}")
        os.replace(tmp, out_path)

def parquet_meta_local(path: str) -> Dict:
    pf = ParquetFile(path)  # lit le footer uniquement
    schema = pf.schema_arrow
    m = pf.metadata
    return {
        "file_name": os.path.basename(path),
        "file_size_bytes": os.path.getsize(path),
        "num_rows": m.num_rows,
        "num_row_groups": m.num_row_groups,
        "num_columns": m.num_columns,
        "created_by": m.created_by,
        "columns": [f.name for f in schema],
        "column_types": [str(f.type) for f in schema],
    }

def meta_json_path(folder: str, color: str, year: int, month: int) -> str:
    return os.path.join(folder, f"metadata_{color}_{year}-{month:02d}.json")

# -------- DAG Airflow 3 (TaskFlow API) ----------
@dag(
    dag_id="taxi_bulk_download_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,           
    catchup=False,
    tags=["taxi_yellow"],
)
def taxi_bulk_download_dag():

    @task
    def process_month(color: str, year: int, month: int) -> None:
        session = make_session()
        try:
            url = f"{BASE_URL}/{color}_tripdata_{year}-{month:02d}.parquet"
            folder = ensure_folder(BASE_FOLDER, color, year)
            parquet_out = os.path.join(folder, f"{color}_tripdata_{year}-{month:02d}.parquet")
            json_out = meta_json_path(folder, color, year, month)

            # 1) HEAD
            h = head_info(session, url)
            expected = int(h["content_length"]) if h["content_length"] else None
            print(f"ℹ️ [{color} {year}-{month:02d}] HEAD OK — CL={h.get('content_length')}")

            # 2) Parquet: existe & taille OK → skip download
            if os.path.exists(parquet_out) and expected is not None and os.path.getsize(parquet_out) == expected:
                print(f"↪️  Déjà présent et complet : {parquet_out}")
            else:
                download_stream(session, url, parquet_out, expected)
                print(f"✅ Téléchargé : {parquet_out}")

            # 3) Metadata JSON: éviter écrasement
            if os.path.exists(json_out):
                print(f"↪️  Metadata déjà présente : {json_out}")
            else:
                meta = parquet_meta_local(parquet_out)
                payload = {"http_head": h, "parquet": meta, "year": year, "color": color}
                with open(json_out, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                print(f"✅ Métadonnées : {json_out}")

            # 4) Pause entre fichiers (politesse)
            time.sleep(SLEEP_BETWEEN_FILES)
        finally:
            session.close()

    start_year, end_year = YEAR_RANGE
    for year in range(start_year, end_year + 1):
        for color in COLORS:
            for m in MONTHS:
                process_month.override(task_id=f"dl_{color}_{year}_{m:02d}")(color, year, m)

taxi_bulk_download_dag()