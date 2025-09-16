# dags/transform_yellow_parquet.py
from __future__ import annotations
import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id="transform_yellow_parquet",   
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["polars", "nyc-taxi", "transformation", "idempotent"],
)
def transform_yellow_parquet():
    @task
    def list_years() -> list[int]:
        return list(range(2012, 2026))

    @task
    def transform_year(year: int) -> int:
        from pathlib import Path
        from utils_transformations_yellow import (
            year_paths, pending_pairs, transform_one_file
        )
        in_root, out_root = year_paths(year)
        if not in_root.exists():
            print(f"[{year}] Dossier absent: {in_root}")
            return 0
        todo: list[tuple[Path, Path]] = pending_pairs(in_root, out_root)
        if not todo:
            print(f"[{year}] Rien à faire.")
            return 0
        written = 0
        for src, dst in todo:
            if transform_one_file(src, dst):
                written += 1
        print(f"[{year}] OK — {written} fichier(s)")
        return written

    @task
    def summarize(counts: list[int]) -> None:
        print("Total fichiers écrits:", sum(counts))

    years = list_years()
    results = transform_year.expand(year=years)
    summarize(results)

dag = transform_yellow_parquet()
