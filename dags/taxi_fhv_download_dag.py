# dags/dag_download_fvh.py
from __future__ import annotations

from airflow.decorators import dag, task
from datetime import datetime

from utils_download_fvh import (
    load_fhv_items_from_csv, make_session, ensure_folder_staging,
    head_info, download_stream, parquet_meta_local,
    meta_json_path, out_parquet_path,
)

@dag(
    dag_id="fhv_download_from_csv_daily",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["download", "fhv", "parquet", "from_csv", "idempotent"],
)
def fhv_download_from_csv():

    @task
    def list_items() -> list[dict]:
        """
        Charge la liste des (year, month, url) FHV depuis le CSV.
        """
        items = load_fhv_items_from_csv()
        print(f"{len(items)} éléments FHV trouvés dans le CSV.")
        # Exemple d'item: {'year': 2019, 'month': 2, 'url': 'https://.../fhv_tripdata_2019-02.parquet'}
        return items

    @task
    def download_item(item: dict) -> str | None:
        """
        Télécharge un item (year, month, url) si manquant.
        Écrit aussi le JSON de métadonnées. Idempotent via Content-Length.
        """
        import os
        import time

        year: int = int(item["year"])
        month: int = int(item["month"])
        url: str = str(item["url"])

        session = make_session()
        try:
            folder = ensure_folder_staging(year)
            parquet_out = out_parquet_path(folder, year, month)
            json_out = meta_json_path(folder, year, month)

            # HEAD
            h = head_info(session, url)
            expected = int(h["content_length"]) if h["content_length"] else None
            print(f"ℹ️ [FHV {year}-{month:02d}] HEAD OK — CL={h.get('content_length')}")

            # Parquet présent & complet -> skip
            if os.path.exists(parquet_out) and expected is not None and os.path.getsize(parquet_out) == expected:
                print(f"↪️  Déjà présent et complet : {parquet_out}")
            else:
                download_stream(session, url, parquet_out, expected)
                print(f"✅ Téléchargé : {parquet_out}")

            # Metadata JSON
            if os.path.exists(json_out):
                print(f"↪️  Metadata déjà présente : {json_out}")
            else:
                meta = parquet_meta_local(parquet_out)
                payload = {"http_head": h, "parquet": meta, "year": year, "dataset": "fhv", "source_url": url}
                with open(json_out, "w", encoding="utf-8") as f:
                    import json
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                print(f"✅ Métadonnées : {json_out}")

            time.sleep(1.0)
            return parquet_out
        finally:
            session.close()

    items = list_items()
    # Dynamic task mapping : une task par lien trouvé dans le CSV
    download_item.expand(item=items)

dag = fhv_download_from_csv()
