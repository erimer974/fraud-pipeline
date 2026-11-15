"""Suivi des performances du pipeline.

Deux rôles :
1. `record_run_metrics` — appelé en dernière tâche de chaque DAG, il écrit une ligne par run
   dans `monitoring.pipeline_runs` : durée, volume traité, rejets, débit, latence de l'API,
   fraudes, version du modèle, statut. Grafana lit cette table en temps réel.
2. `evaluate_health` — règles d'alerte appliquées par le DAG `pipeline_monitoring` sur ces
   métriques. Fonction pure, testée dans `tests/test_monitoring.py`.

Ajouter une métrique = ajouter une clé dans `extra` (colonne `extra` JSONB, aucune migration
de schéma) puis un panneau dans Grafana. Ajouter une règle = une entrée dans
`evaluate_health` et un seuil dans `DEFAULTS["monitoring_thresholds"]`.
"""
import json
import logging
from datetime import UTC, datetime

log = logging.getLogger("airflow.task")

FAILED_STATES = {"failed", "upstream_failed"}

# Colonnes dédiées de `monitoring.pipeline_runs` ; toute autre clé va dans `extra`.
METRIC_COLUMNS = [
    "rows_in", "rows_ok", "rows_rejected", "frauds", "rows_per_s", "api_latency_ms",
    "model_version",
]

INSERT_RUN_SQL = """
    INSERT INTO monitoring.pipeline_runs
        (dag_id, run_id, logical_date, started_at, ended_at, duration_s, status, failed_tasks,
         rows_in, rows_ok, rows_rejected, frauds, rows_per_s, api_latency_ms, model_version, extra)
    VALUES (%(dag_id)s, %(run_id)s, %(logical_date)s, %(started_at)s, %(ended_at)s, %(duration_s)s,
            %(status)s, %(failed_tasks)s, %(rows_in)s, %(rows_ok)s, %(rows_rejected)s, %(frauds)s,
            %(rows_per_s)s, %(api_latency_ms)s, %(model_version)s, %(extra)s)
    ON CONFLICT (dag_id, run_id) DO UPDATE SET
        ended_at = EXCLUDED.ended_at, duration_s = EXCLUDED.duration_s, status = EXCLUDED.status,
        failed_tasks = EXCLUDED.failed_tasks, rows_in = EXCLUDED.rows_in, rows_ok = EXCLUDED.rows_ok,
        rows_rejected = EXCLUDED.rows_rejected, frauds = EXCLUDED.frauds,
        rows_per_s = EXCLUDED.rows_per_s, api_latency_ms = EXCLUDED.api_latency_ms,
        model_version = EXCLUDED.model_version, extra = EXCLUDED.extra
"""


def build_run_record(dag_id, run_id, logical_date, started_at, ended_at, task_states, metrics):
    """Construit la ligne de métriques d'un run (fonction pure, testable).

    `task_states` : {task_id: état} des autres tâches du run.
    Statut : `failed` si une tâche a échoué, `rejected` si la donnée a été mise en
    quarantaine sans erreur technique, `success` sinon.
    """
    failed = sorted(t for t, s in task_states.items() if s in FAILED_STATES)
    metrics = dict(metrics)
    if failed:
        status = "failed"
    elif metrics.get("rows_rejected") and not metrics.get("rows_ok"):
        status = "rejected"
    else:
        status = "success"

    record = {c: metrics.pop(c, None) for c in METRIC_COLUMNS}
    record.update(
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        started_at=started_at,
        ended_at=ended_at,
        duration_s=round((ended_at - started_at).total_seconds(), 3) if started_at else None,
        status=status,
        failed_tasks=",".join(failed) or None,
        extra=json.dumps(metrics, default=str),
    )
    return record


def record_run_metrics(context, metrics: dict) -> dict:
    """Enregistre les métriques du run courant, puis fait échouer la tâche si le run a échoué.

    Cette tâche est la dernière du DAG (trigger_rule="all_done") : elle s'exécute toujours.
    Sans la levée d'exception finale, un run dont `fetch_transactions` a échoué finirait
    « vert », car Airflow juge l'état d'un run sur ses tâches terminales.
    """
    from airflow.exceptions import AirflowFailException

    from fraud_pipeline import storage

    dag_run = context["dag_run"]
    me = context["ti"].task_id
    task_states = {
        ti.task_id: ti.state for ti in dag_run.get_task_instances() if ti.task_id != me
    }
    record = build_run_record(
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        logical_date=context["logical_date"],
        started_at=dag_run.start_date,
        ended_at=datetime.now(UTC),
        task_states=task_states,
        metrics=metrics,
    )
    storage._execute(INSERT_RUN_SQL, record)
    log.info(
        "[METRICS] %s %s — statut=%s durée=%ss lignes=%s",
        record["dag_id"], record["run_id"], record["status"], record["duration_s"], record["rows_ok"],
    )
    if record["status"] == "failed":
        raise AirflowFailException(f"Tâche(s) en échec dans ce run : {record['failed_tasks']}")
    return record


# ── Règles d'alerte ───────────────────────────────────────────

REALTIME_STATS_SQL = """
    SELECT
        count(*)                                                    AS runs,
        count(*) FILTER (WHERE status = 'failed')                   AS failed_runs,
        max(ended_at) FILTER (WHERE status <> 'failed')             AS last_success_at,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_s)    AS duration_p95_s,
        avg(api_latency_ms)                                         AS api_latency_ms,
        coalesce(sum(rows_ok), 0)                                   AS rows_ok,
        coalesce(sum(rows_rejected), 0)                             AS rows_rejected,
        coalesce(sum(frauds), 0)                                    AS frauds
    FROM monitoring.pipeline_runs
    WHERE dag_id = 'fraud_realtime'
      AND ended_at > now() - make_interval(mins => %s)
"""

LAST_REALTIME_SUCCESS_SQL = """
    SELECT max(ended_at) FROM monitoring.pipeline_runs
    WHERE dag_id = 'fraud_realtime' AND status <> 'failed'
"""

DAILY_REPORT_SQL = """
    SELECT 1 FROM monitoring.pipeline_runs
    WHERE dag_id = 'fraud_daily_report' AND status = 'success'
      AND logical_date::date = (now() AT TIME ZONE 'UTC')::date - 1
    LIMIT 1
"""


def evaluate_health(stats: dict, thresholds: dict, now: datetime) -> list:
    """Applique les règles d'alerte. Renvoie [(clé, sévérité, message), ...].

    `stats` contient les agrégats de la fenêtre d'observation (voir REALTIME_STATS_SQL),
    plus `last_success_at` (dernier run réussi, toutes périodes) et `daily_report_ok`.
    """
    alerts = []
    t = thresholds

    last = stats.get("last_success_at")
    if last is None or (now - last).total_seconds() > t["freshness_min"] * 60:
        since = "jamais" if last is None else f"depuis {int((now - last).total_seconds() // 60)} min"
        alerts.append((
            "health:freshness", "critical",
            f"Aucun run temps réel réussi {since} (seuil : {t['freshness_min']} min). "
            f"Pipeline arrêté, en pause ou en échec continu.",
        ))

    runs = stats.get("runs") or 0
    if runs:
        error_rate = 100 * (stats.get("failed_runs") or 0) / runs
        if error_rate > t["error_rate_pct"]:
            alerts.append((
                "health:error_rate", "error",
                f"Taux d'échec {error_rate:.0f} % sur {runs} runs ({t['window_min']} min) "
                f"— seuil {t['error_rate_pct']} %.",
            ))

    p95 = stats.get("duration_p95_s")
    if p95 is not None and p95 > t["duration_p95_s"]:
        alerts.append((
            "health:duration", "warning",
            f"Durée des runs dégradée : p95 = {p95:.1f} s (seuil {t['duration_p95_s']} s).",
        ))

    latency = stats.get("api_latency_ms")
    if latency is not None and latency > t["api_latency_ms"]:
        alerts.append((
            "health:api_latency", "warning",
            f"API lente : latence moyenne {latency:.0f} ms (seuil {t['api_latency_ms']} ms).",
        ))

    rows_ok = stats.get("rows_ok") or 0
    rows_rejected = stats.get("rows_rejected") or 0
    total = rows_ok + rows_rejected
    if total and 100 * rows_rejected / total > t["rejected_rate_pct"]:
        alerts.append((
            "health:rejected_rate", "warning",
            f"Qualité des données : {rows_rejected}/{total} transactions rejetées "
            f"(seuil {t['rejected_rate_pct']} %). Voir la table rejected_transactions.",
        ))

    if rows_ok >= 20 and 100 * (stats.get("frauds") or 0) / rows_ok > t["fraud_rate_pct"]:
        alerts.append((
            "health:fraud_rate", "warning",
            f"Taux de fraude prédit anormal : {stats['frauds']}/{rows_ok} "
            f"(seuil {t['fraud_rate_pct']} %). Dérive du modèle ou attaque en cours ?",
        ))

    if now.hour >= t["daily_report_hour_utc"] and not stats.get("daily_report_ok"):
        alerts.append((
            "health:daily_report", "error",
            f"Le rapport quotidien de la veille n'a pas été produit (attendu avant "
            f"{t['daily_report_hour_utc']}h UTC).",
        ))
    return alerts
