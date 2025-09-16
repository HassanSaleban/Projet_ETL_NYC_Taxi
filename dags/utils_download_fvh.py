from __future__ import annotations

import os
import re
import time
import json
import csv
from typing import Optional, Dict, Iterable, Tuple, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyarrow.parquet import ParquetFile

# ================== Réglages ==================
# Chemins POSIX dans le conteneur (configurables par env vars)
STAGING_BASE = os.getenv("STAGING_BASE", "/opt/airflow/exports/staging")
FOLDER_NAME = "fhv"

# CSV contenant les liens TLC (monte ton dossier scripts dans Docker)
LINKS_CSV = os.getenv(
    "LINKS_CSV",
    "/opt/airflow/scripts/nyc_tlc_parquet_links.csv",
)

# Réseau
DEFAULT_TIMEOUT = (5, 30)
MAX_RETRIES = 5
BACKOFF = 1.2
SLEEP_BETWEEN = 1.0
SLEEP_BETWEEN_FILES = 1.0
USER_AGENT = "TaxiDataFetcher/2.1 (+Airflow/FHV)"
# ============================================

# Regex robuste: capture url + (year, month)
FHV_LINK_RE = re.compile(
    r'(https?://[^\s,"]*?fhv_tripdata_(\d{4})-(\d{2})\.parquet)',
    re.IGNORECASE,
)

# ============== Lecture CSV -> items FHV ==============

def load_fhv_items_from_csv(csv_path: str = LINKS_CSV) -> List[dict]:
    """
    Lit le CSV de liens TLC et renvoie une liste d'items FHV:
    [{ 'year': 2019, 'month': 2, 'url': 'https://.../fhv_tripdata_2019-02.parquet' }, ...]
    Le CSV peut avoir n'importe quelles colonnes: on scanne chaque cellule pour une URL FHV.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV introuvable: {csv_path}")

    items: List[dict] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            for cell in row:
                if not cell:
                    continue
                m = FHV_LINK_RE.search(cell)
                if m:
                    url, year_str, month_str = m.group(1), m.group(2), m.group(3)
                    items.append({
                        "year": int(year_str),
                        "month": int(month_str),
                        "url": url,
                    })
    # dédoublonnage / tri (au cas où)
    seen = set()
    unique: List[dict] = []
    for it in items:
        key = (it["year"], it["month"], it["url"])
        if key not in seen:
            seen.add(key)
            unique.append(it)
    unique.sort(key=lambda d: (d["year"], d["month"]))
    return unique

# ============== Réseau / IO ==============

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


def ensure_folder_staging(year: int) -> str:
    """Crée /staging/fhv/<year> et renvoie le chemin POSIX."""
    path = os.path.join(STAGING_BASE, FOLDER_NAME, str(year))
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
    """Téléchargement streaming avec écriture atomique et contrôle de taille."""
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
    """Lit la métadonnée parquet sans charger les lignes (footer only)."""
    pf = ParquetFile(path)
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


def meta_json_path(folder: str, year: int, month: int) -> str:
    return os.path.join(folder, f"metadata_fhv_{year}-{month:02d}.json")


def out_parquet_path(folder: str, year: int, month: int) -> str:
    return os.path.join(folder, f"fhv_tripdata_{year}-{month:02d}.parquet")


__all__ = [
    # config
    "STAGING_BASE", "FOLDER_NAME", "LINKS_CSV",
    # extraction CSV
    "load_fhv_items_from_csv",
    # network/io
    "make_session", "ensure_folder_staging",
    "head_info", "download_stream",
    "parquet_meta_local", "meta_json_path", "out_parquet_path",
]
