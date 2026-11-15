"""DAG 4 — Surveillance du pipeline et détection d'incidents (toutes les 10 minutes).

check_pipeline_health   ─┐
                         ├→ (alertes Discord dédupliquées, historisées dans monitoring.alerts)
check_security          ─┘

- check_pipeline_health : applique les règles d'alerte aux métriques des runs (fraîcheur,
  taux d'échec, durée, latence API, rejets qualité, dérive du taux de fraude, rapport
  quotidien manquant). Les seuils sont la Variable Airflow `monitoring_thresholds` (JSON) :
  modifiables à chaud, sans redéploiement.
- check_security : rôles et sessions PostgreSQL non autorisés, disparition de données non
  journalisée. Tout incident est consigné dans `audit.journal` (registre des violations).

Ce DAG surveille les autres : il est volontairement indépendant d'eux (aucune dépendance
Airflow), pour continuer à alerter même si les DAGs surveillés sont bloqués.
"""
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator

from fraud_pipeline import monitoring, security, storage
from fraud_pipeline.alerts import notify_failure, send_deduplicated
from fraud_pipeline.config import get_setting, log, security_allowed_roles

SEVERITY_ICON = {"critical": "🔴", "error": "🟠", "warning": "🟡"}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure,
}


def check_pipeline_health(**context):
    thresholds = get_setting("monitoring_thresholds")
    columns = ["runs", "failed_runs", "last_success_at", "duration_p95_s", "api_latency_ms",
               "rows_ok", "rows_rejected", "frauds"]
    row = storage.fetch_all(monitoring.REALTIME_STATS_SQL, (thresholds["window_min"],))[0]
    stats = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in zip(columns, row, strict=False)}
    stats["last_success_at"] = storage.fetch_all(monitoring.LAST_REALTIME_SUCCESS_SQL)[0][0]
    stats["daily_report_ok"] = bool(storage.fetch_all(monitoring.DAILY_REPORT_SQL))

    alerts = monitoring.evaluate_health(stats, thresholds, datetime.now(UTC))
    log.info("[HEALTH] stats=%s → %s alerte(s)", stats, len(alerts))
    for key, severity, message in alerts:
        send_deduplicated(
            key, severity,
            f"{SEVERITY_ICON[severity]} **ALERTE PIPELINE — {severity.upper()}**\n{message}",
            thresholds["realert_min"],
        )
    context["ti"].xcom_push(key="alerts", value=[a[0] for a in alerts])


def check_security(**context):
    thresholds = get_setting("monitoring_thresholds")
    incidents = security.run_security_checks(security_allowed_roles())
    for key, message in incidents:
        storage.journal("incident_securite", "pipeline_monitoring", key, {"message": message})
        send_deduplicated(
            key, "critical",
            f"🚨 **SÉCURITÉ — INCIDENT POTENTIEL SUR LES DONNÉES**\n{message}\n"
            f"Procédure : docs/RGPD.md § Violation de données (qualification sous 24 h, "
            f"notification CNIL sous 72 h si risque pour les personnes).",
            thresholds["realert_min"],
        )
    log.info("[SECURITY] %s incident(s) détecté(s)", len(incidents))


with DAG(
    dag_id="pipeline_monitoring",
    description="Règles d'alerte sur les métriques du pipeline et contrôles de sécurité",
    default_args=default_args,
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["monitoring", "sécurité"],
    doc_md=__doc__,
) as dag:

    PythonOperator(task_id="check_pipeline_health", python_callable=check_pipeline_health)
    PythonOperator(task_id="check_security", python_callable=check_security)
