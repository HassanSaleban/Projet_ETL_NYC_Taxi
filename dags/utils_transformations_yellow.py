
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import polars as pl

# =============================================================================
# 1) Chemins (configurables par variables d'environnement)
# =============================================================================

IN_BASE: Path = Path(os.getenv("IN_BASE", "/opt/airflow/exports/staging/yellow"))
OUT_BASE: Path = Path(os.getenv("OUT_BASE", "/opt/airflow/exports/transformation/yellow"))

# =============================================================================
# 2) Schéma cible et mapping de renommage
# =============================================================================

SCHEMA_TARGET: Dict[str, pl.DataType] = {
    "VendorID":               pl.Int32,
    "tpep_pickup_datetime":   pl.Datetime(time_unit="us"),
    "tpep_dropoff_datetime":  pl.Datetime(time_unit="us"),
    "passenger_count":        pl.Int64,
    "trip_distance":          pl.Float64,
    "RatecodeID":             pl.Int64,
    "store_and_fwd_flag":     pl.Utf8,
    "PULocationID":           pl.Int32,
    "DOLocationID":           pl.Int32,
    "payment_type":           pl.Int64,
    "fare_amount":            pl.Float64,
    "extra":                  pl.Float64,
    "mta_tax":                pl.Float64,
    "tip_amount":             pl.Float64,
    "tolls_amount":           pl.Float64,
    "improvement_surcharge":  pl.Float64,
    "total_amount":           pl.Float64,
    "congestion_surcharge":   pl.Float64,
    "Airport_fee":            pl.Float64,
    # Exemple colonne récente :
    # "cbd_congestion_fee":     pl.Float64,
}

RENAME_MAP: Dict[str, str] = {
    "VendorID": "VendorID",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "RatecodeID",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "PULocationID",
    "DOLocationID": "DOLocationID",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "Airport_fee",
}

# =============================================================================
# 3) Fonctions utilitaires (sans exécution automatique)
# =============================================================================

def apply_rename_and_schema(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Renomme, caste, crée les colonnes manquantes et réordonne selon SCHEMA_TARGET."""
    present = set(lf.columns)
    rename_submap = {src: dst for src, dst in RENAME_MAP.items()
                     if src in present and src != dst}
    if rename_submap:
        lf = lf.rename(rename_submap)

    present_after = set(lf.columns)
    casts = [pl.col(col).cast(dtype)
             for col, dtype in SCHEMA_TARGET.items() if col in present_after]

    missing = [pl.lit(None).cast(dtype).alias(col)
               for col, dtype in SCHEMA_TARGET.items() if col not in present_after]

    lf = lf.with_columns(casts + missing)
    return lf.select(list(SCHEMA_TARGET.keys()))


def year_paths(year: int) -> Tuple[Path, Path]:
    """Retourne (in_root, out_root) pour une année donnée, sans créer de dossiers."""
    return IN_BASE / str(year), OUT_BASE / str(year)


def iter_parquet_files(root: Path) -> Iterable[Path]:
    """Itère sur les .parquet directement sous `root` (non récursif)."""
    return sorted(root.glob("*.parquet"))


def pending_pairs(in_root: Path, out_root: Path) -> list[tuple[Path, Path]]:
    """
    Retourne la liste (src, dst) à traiter : uniquement les fichiers dont la
    sortie N'EXISTE PAS encore.
    """
    out_root.mkdir(parents=True, exist_ok=True)
    pairs: list[tuple[Path, Path]] = []
    for src in iter_parquet_files(in_root):
        dst = out_root / src.name
        if not dst.exists():
            pairs.append((src, dst))
    return pairs


def pending_pairs_changed(in_root: Path, out_root: Path) -> list[tuple[Path, Path]]:
    """
    Variante : retourne (src, dst) si la sortie est absente OU si la source
    est plus récente que la sortie (rebuild sélectif).
    """
    out_root.mkdir(parents=True, exist_ok=True)
    pairs: list[tuple[Path, Path]] = []
    for src in iter_parquet_files(in_root):
        dst = out_root / src.name
        if (not dst.exists()) or (src.stat().st_mtime > dst.stat().st_mtime):
            pairs.append((src, dst))
    return pairs


def transform_one_file(src: Path, dst: Path, overwrite: bool = False) -> bool:
    """
    Transforme un fichier unique (lecture lazy -> cast -> écriture).
    - Idempotent : si dst existe et overwrite=False, on skip.
    - Écriture atomique : écrit d'abord dans dst+'.tmp', puis remplace.

    Retourne True si un fichier a été écrit, False si sauté.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not overwrite:
        print(f"skip (exists): {dst}")
        return False

    tmp = dst.with_suffix(dst.suffix + ".tmp")
    lf = pl.scan_parquet(str(src))
    lf = apply_rename_and_schema(lf)
    lf.sink_parquet(str(tmp))
    os.replace(tmp, dst)  # remplace de façon atomique
    return True


__all__ = [
    "IN_BASE",
    "OUT_BASE",
    "SCHEMA_TARGET",
    "RENAME_MAP",
    "apply_rename_and_schema",
    "year_paths",
    "iter_parquet_files",
    "pending_pairs",
    "pending_pairs_changed",
    "transform_one_file",
]
