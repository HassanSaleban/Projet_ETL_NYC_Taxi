# dags/yellow_concat_dag.py
from __future__ import annotations

import os
from pathlib import Path
import pendulum
from airflow.decorators import dag, task


# --- CONFIG CHEMINS ---
# Monte ce chemin hôte dans le conteneur, puis règle ces env vars.
# Exemples docker-compose:
#   - "C:/Users/.../Airflow/exports:/opt/airflow/exports"
IN_BASE  = Path(os.getenv("YELLOW_TRANSFORM_BASE", "/opt/airflow/exports/transformation/yellow"))
OUT_BASE = Path(os.getenv("YELLOW_CONCAT_BASE",    "/opt/airflow/exports/concatenation"))


@dag(
    dag_id="yellow_concat_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nyc-taxi", "polars", "concat", "idempotent"],
)
def yellow_concat_dag():
    """Concatène les Parquet mensuels de 2012 à 2025 en un fichier par année."""

    @task
    def list_years() -> list[int]:
        return list(range(2012, 2026))  # 2012..2025 inclus

    @task
    def concat_year(year: int) -> str | None:
        """
        Concatène tous les .parquet de IN_BASE/<year> vers OUT_BASE/yellow_year_<year>.parquet
        - union 'diagonal' pour gérer schema variable
        - idempotent : ne réécrit pas si la sortie existe déjà
        Retourne le chemin de sortie (str) ou None si rien écrit.
        """
        import polars as pl

        in_dir  = IN_BASE / str(year)
        out_dir = OUT_BASE
        out_dir.mkdir(parents=True, exist_ok=True)

        if not in_dir.exists():
            print(f"[{year}] Dossier absent: {in_dir}")
            return None

        files = sorted(in_dir.glob("*.parquet"))
        if not files:
            print(f"[{year}] Aucun parquet dans {in_dir}")
            return None

        dst = out_dir / f"yellow_year_{year}.parquet"
        if dst.exists():  # idempotent
            print(f"[{year}] Déjà présent, skip -> {dst}")
            return str(dst)

        # Concat lazy (remplit NULL pour les colonnes manquantes)
        lfs = [pl.scan_parquet(str(p)) for p in files]
        lf_all = pl.concat(lfs, how="diagonal")

        # Écriture (streaming)
        lf_all.sink_parquet(str(dst))
        print(f"[{year}] OK -> {dst}")
        return str(dst)

    @task
    def summarize(outputs: list[str | None]) -> None:
        written = [p for p in outputs if p]
        print(f"Fichiers écrits/présents : {len(written)}")
        for p in written:
            print(" -", p)

    years = list_years()
    outs = concat_year.expand(year=years)
    summarize(outs)


dag = yellow_concat_dag()
