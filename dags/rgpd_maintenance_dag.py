"""DAG 5 — Maintenance RGPD et sauvegarde (chaque nuit à 2h).

backup_database → purge_expired_data → purge_old_files

- backup_database : sauvegarde logique des tables (CSV compressés dans `data/backups/`,
  rotation sur `backups_kept` jours). Faite AVANT la purge : on peut toujours restaurer la
  veille.
- purge_expired_data : supprime les transactions plus anciennes que `retention_days`
  (90 jours par défaut) et la quarantaine au-delà de `rejected_retention_days`.
- purge_old_files : supprime rapports et exports RGPD au-delà de `files_retention_days`.

Toutes les durées sont des Variables Airflow ; chaque opération est tracée dans `audit.journal`.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fraud_pipeline import rgpd
from fraud_pipeline.alerts import notify_failure
from fraud_pipeline.config import REPORTS_DIR, RGPD_EXPORT_DIR, get_setting, log

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


def backup_database(**context):
    files = rgpd.backup_tables(context["ds"], get_setting("backups_kept"))
    log.info("[BACKUP] %s fichier(s) écrit(s) : %s", len(files), files)


def purge_expired_data(**context):
    result = rgpd.purge_expired(get_setting("retention_days"), get_setting("rejected_retention_days"))
    log.info("[PURGE] %s", result)


def purge_old_files(**context):
    removed = rgpd.purge_old_files([REPORTS_DIR, RGPD_EXPORT_DIR], get_setting("files_retention_days"))
    log.info("[PURGE] %s fichier(s) supprimé(s)", removed)


with DAG(
    dag_id="rgpd_maintenance",
    description="Sauvegarde, purge des données échues (durée de conservation RGPD)",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rgpd", "maintenance"],
    doc_md=__doc__,
) as dag:

    t_backup = PythonOperator(task_id="backup_database", python_callable=backup_database)
    t_purge = PythonOperator(task_id="purge_expired_data", python_callable=purge_expired_data)
    t_files = PythonOperator(task_id="purge_old_files", python_callable=purge_old_files)

    t_backup >> t_purge >> t_files
