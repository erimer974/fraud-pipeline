"""Alertes Discord partagées par tous les DAGs.

🔒 Le webhook n'est JAMAIS écrit dans le code : il est lu dans la variable d'environnement
DISCORD_WEBHOOK_URL, définie dans `.env` (non versionné, voir `.env.example`) et injectée par
docker-compose. Si elle est absente, les alertes sont ignorées avec un log.

Règle commune : une alerte ne fait JAMAIS échouer la tâche qui l'émet. Si Discord est
indisponible, la transaction doit malgré tout être analysée et stockée.

Trois familles d'alertes, préfixées pour être triées d'un coup d'œil dans le canal :
FRAUDE (métier), PIPELINE (technique), SÉCURITÉ (incident de données).
"""
import logging
import os

import requests

log = logging.getLogger("airflow.task")

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Délai minimal entre deux alertes d'échec identiques (même DAG, même tâche) : un échec
# qui se répète à chaque run minuté ne doit pas saturer le canal.
FAILURE_REALERT_MIN = 30


def send_discord(message: str) -> bool:
    """Envoie un message Discord sans jamais lever d'exception. Renvoie True si envoyé."""
    if not DISCORD_WEBHOOK_URL:
        log.warning("[DISCORD] DISCORD_WEBHOOK_URL non définie — notification ignorée.")
        return False
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
        response.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001 — une alerte ne doit jamais faire échouer une tâche
        log.error("[DISCORD] Échec de l'envoi : %s", exc)
        return False


def send_deduplicated(alert_key: str, severity: str, message: str, realert_min: int) -> bool:
    """Envoie l'alerte sauf si la même clé a déjà été émise dans les `realert_min` minutes.

    Chaque alerte émise est enregistrée dans `monitoring.alerts` : c'est à la fois le
    mécanisme de déduplication et l'historique consultable dans Grafana.
    """
    from fraud_pipeline import storage

    try:
        if storage.alert_recently_sent(alert_key, realert_min):
            log.info("[ALERTE] %s déjà émise il y a moins de %s min — ignorée.", alert_key, realert_min)
            return False
        storage.record_alert(alert_key, severity, message)
    except Exception as exc:  # noqa: BLE001 — base indisponible : on alerte quand même
        log.error("[ALERTE] Déduplication impossible (%s) — envoi direct.", exc)
    return send_discord(message)


def notify_failure(context) -> None:
    """on_failure_callback : prévient sur Discord qu'une tâche a échoué (dédupliqué)."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "?"
    task_id = ti.task_id if ti else "?"
    log_url = getattr(ti, "log_url", "")
    exception = context.get("exception")
    send_deduplicated(
        alert_key=f"failure:{dag_id}:{task_id}",
        severity="error",
        message=(
            f"**ÉCHEC PIPELINE**\n"
            f"DAG : `{dag_id}`\n"
            f"Tâche : `{task_id}`\n"
            f"Erreur : {str(exception)[:300] if exception else 'inconnue'}\n"
            f"Logs : {log_url}"
        ),
        realert_min=FAILURE_REALERT_MIN,
    )
