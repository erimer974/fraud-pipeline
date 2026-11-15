"""DAG 3 — Ingestion batch de fichiers de transactions (déclenchée par dépôt de fichier).

detect_new_files → ingest_files → record_metrics

Deuxième source de données, à côté de l'API : un fichier déposé dans `data/inbox/` est pris
en charge automatiquement dans les 5 minutes. Il démontre trois capacités du pipeline :

- **Formats variés** : CSV, JSON (tableau ou JSON Lines) et Parquet.
- **Volumes variables** : lecture par lots de 50 000 lignes, mémoire bornée quelle que soit la
  taille du fichier. Le débit (lignes/s) est mesuré et enregistré à chaque run.
- **Robustesse** : les lignes invalides partent en quarantaine sans bloquer les autres ; un
  fichier illisible est déplacé dans `data/error/` et alerté, les autres fichiers continuent.

Banc d'essai : déposer une copie de `fraudTest.csv` (555 719 lignes) dans `data/inbox/`.
"""
import os
import shutil
import time
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from fraud_pipeline import model, monitoring, quality, storage
from fraud_pipeline.alerts import notify_failure, send_discord
from fraud_pipeline.config import (
    ARCHIVE_DIR,
    BATCH_CHUNK_SIZE,
    BATCH_FORMATS,
    CHAMPS_CONSERVES,
    ERROR_DIR,
    INBOX_DIR,
    get_setting,
    log,
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure,
}


def read_in_chunks(path: str, chunk_size: int = BATCH_CHUNK_SIZE):
    """Lit un fichier par lots, quel que soit son format. Seules les colonnes autorisées sont lues."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        yield from pd.read_csv(path, usecols=lambda c: c in CHAMPS_CONSERVES, chunksize=chunk_size)
    elif ext == ".jsonl":
        for chunk in pd.read_json(path, lines=True, chunksize=chunk_size):
            yield chunk[[c for c in CHAMPS_CONSERVES if c in chunk.columns]]
    elif ext == ".json":
        df = pd.read_json(path)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i + chunk_size][[c for c in CHAMPS_CONSERVES if c in df.columns]]
    elif ext == ".parquet":
        df = pd.read_parquet(path, columns=None)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i + chunk_size][[c for c in CHAMPS_CONSERVES if c in df.columns]]
    else:
        raise ValueError(f"Format non pris en charge : {ext}")


# ── Tâche 1 : Détecter les fichiers déposés (déclencheur) ─────
def detect_new_files(**context):
    os.makedirs(INBOX_DIR, exist_ok=True)
    files = sorted(
        os.path.join(INBOX_DIR, f) for f in os.listdir(INBOX_DIR)
        if f.lower().endswith(BATCH_FORMATS)
    )
    context["ti"].xcom_push(key="files", value=files)
    log.info("[DETECT] %s fichier(s) en attente : %s", len(files), files)
    return bool(files)   # False → les tâches suivantes sont ignorées (aucun fichier)


# ── Tâche 2 : Ingérer, contrôler, scorer, stocker ─────────────
def ingest_files(**context):
    ti = context["ti"]
    files = ti.xcom_pull(key="files", task_ids="detect_new_files") or []
    threshold = get_setting("fraud_threshold")
    version = model.model_version()
    totals = {"rows_in": 0, "rows_ok": 0, "rows_rejected": 0, "frauds": 0, "files_ok": 0, "files_error": 0}
    start = time.perf_counter()

    for path in files:
        name = os.path.basename(path)
        source = f"batch:{name}"
        try:
            for chunk in read_in_chunks(path):
                valid, rejected = quality.split_valid_rejected(chunk)
                if valid:
                    probas = model.predict_proba(valid)
                    for row, p in zip(valid, probas, strict=False):
                        row.update(
                            fraud_proba=round(p, 5),
                            is_fraud_predicted=int(p >= threshold),
                            model_version=version,
                            source=source,
                            dag_run_id=context["run_id"],
                        )
                    storage.upsert_transactions(valid)
                storage.insert_rejected(rejected, source, context["run_id"])
                totals["rows_in"] += len(chunk)
                totals["rows_ok"] += len(valid)
                totals["rows_rejected"] += len(rejected)
                totals["frauds"] += sum(r["is_fraud_predicted"] for r in valid)
            os.makedirs(ARCHIVE_DIR, exist_ok=True)
            shutil.move(path, os.path.join(ARCHIVE_DIR, f"{datetime.now():%Y%m%d_%H%M%S}_{name}"))
            totals["files_ok"] += 1
            log.info("[INGEST] %s traité → archive", name)
        except Exception as exc:  # noqa: BLE001 — un fichier en erreur ne bloque pas les autres
            os.makedirs(ERROR_DIR, exist_ok=True)
            shutil.move(path, os.path.join(ERROR_DIR, name))
            totals["files_error"] += 1
            log.error("[INGEST] %s illisible (%s) → data/error/", name, exc)
            send_discord(f"**ÉCHEC INGESTION**\nFichier : `{name}`\nErreur : {str(exc)[:300]}\n"
                         f"Le fichier a été déplacé dans `data/error/`.")

    elapsed = time.perf_counter() - start
    totals["elapsed_s"] = round(elapsed, 2)
    totals["rows_per_s"] = round(totals["rows_in"] / elapsed, 1) if elapsed and totals["rows_in"] else None
    ti.xcom_push(key="totals", value=totals)
    log.info("[INGEST] Bilan : %s", totals)
    if totals["files_error"] and not totals["files_ok"]:
        raise RuntimeError("Aucun fichier n'a pu être ingéré.")


# ── Tâche 3 : Métriques du run ────────────────────────────────
def record_metrics(**context):
    totals = context["ti"].xcom_pull(key="totals", task_ids="ingest_files")
    if totals is None:
        return   # aucun fichier déposé : rien à mesurer
    monitoring.record_run_metrics(context, {**totals, "model_version": model.model_version()})


with DAG(
    dag_id="fraud_batch_ingest",
    description="Ingestion des fichiers de transactions déposés dans data/inbox/",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraude", "batch", "ingestion"],
    doc_md=__doc__,
) as dag:

    t_detect = ShortCircuitOperator(
        task_id="detect_new_files",
        python_callable=detect_new_files,
        ignore_downstream_trigger_rules=False,   # record_metrics (all_done) s'exécute quand même
    )
    t_ingest = PythonOperator(task_id="ingest_files", python_callable=ingest_files,
                              execution_timeout=timedelta(minutes=30))
    t_metrics = PythonOperator(
        task_id="record_metrics",
        python_callable=record_metrics,
        trigger_rule="all_done",
        retries=0,
        on_failure_callback=None,
    )

    t_detect >> t_ingest >> t_metrics
