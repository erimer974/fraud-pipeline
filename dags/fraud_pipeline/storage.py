"""Écritures et lectures sur la base métier `fraud-db`.

Toutes les écritures de transactions passent par un UPSERT sur la clé `trans_num` : rejouer
une tâche (nouvelle tentative, run relancé à la main) ne crée jamais de doublon. C'est ce qui
permet au pipeline de reprendre après une erreur sans intervention ni nettoyage.
"""
import json
import logging
import math

from fraud_pipeline.config import POSTGRES_CONN_ID

log = logging.getLogger("airflow.task")

TRANSACTION_COLUMNS = [
    "trans_num", "amt", "merchant", "category", "merch_lat", "merch_long", "city_pop",
    "fraud_proba", "is_fraud_predicted", "model_version", "source", "dag_run_id",
]

UPSERT_TRANSACTIONS_SQL = f"""
    INSERT INTO transactions ({", ".join(TRANSACTION_COLUMNS)})
    VALUES %s
    ON CONFLICT (trans_num) DO UPDATE SET
        fraud_proba        = EXCLUDED.fraud_proba,
        is_fraud_predicted = EXCLUDED.is_fraud_predicted,
        model_version      = EXCLUDED.model_version,
        -- source suit dag_run_id : qui a pris la dernière décision
        source             = EXCLUDED.source,
        dag_run_id         = EXCLUDED.dag_run_id,
        updated_at         = now()
"""


def get_hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(POSTGRES_CONN_ID)


def _execute(sql: str, params=None, fetch: bool = False):
    conn = get_hook().get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if fetch else cur.rowcount
    finally:
        conn.close()


def fetch_all(sql: str, params=None) -> list:
    return _execute(sql, params, fetch=True)


def upsert_transactions(rows: list, page_size: int = 5_000) -> int:
    """Insère ou met à jour un lot de transactions scorées. Renvoie le nombre de lignes."""
    from psycopg2.extras import execute_values

    if not rows:
        return 0
    # Un même trans_num deux fois dans une instruction ferait échouer l'ON CONFLICT :
    # on ne garde que sa dernière occurrence dans le lot.
    unique = {r["trans_num"]: r for r in rows}
    values = [tuple(r[c] for c in TRANSACTION_COLUMNS) for r in unique.values()]
    conn = get_hook().get_conn()
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, UPSERT_TRANSACTIONS_SQL, values, page_size=page_size)
    finally:
        conn.close()
    return len(values)


def insert_rejected(rejected: list, source: str, dag_run_id: str) -> int:
    """Place en quarantaine les enregistrements invalides, avec la raison du rejet."""
    from psycopg2.extras import execute_values

    if not rejected:
        return 0
    values = [
        (
            str(r["record"].get("trans_num") or ""),
            # NaN (champ vide d'un CSV) n'est pas du JSON valide pour PostgreSQL : il devient null.
            json.dumps({k: None if isinstance(v, float) and math.isnan(v) else v
                        for k, v in r["record"].items()}, default=str),
            r["reasons"],
            source,
            dag_run_id,
        )
        for r in rejected
    ]
    conn = get_hook().get_conn()
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO rejected_transactions (trans_num, payload, reasons, source, dag_run_id) "
                "VALUES %s",
                values,
            )
    finally:
        conn.close()
    return len(values)


def journal(event_type: str, actor: str, target: str = "", details: dict = None) -> None:
    """Trace un traitement dans le journal d'audit (`audit.journal`), conservé hors rétention."""
    _execute(
        "INSERT INTO audit.journal (event_type, actor, target, details) VALUES (%s, %s, %s, %s)",
        (event_type, actor, target, json.dumps(details or {}, default=str)),
    )


def alert_recently_sent(alert_key: str, minutes: int) -> bool:
    rows = fetch_all(
        "SELECT 1 FROM monitoring.alerts "
        "WHERE alert_key = %s AND raised_at > now() - make_interval(mins => %s) LIMIT 1",
        (alert_key, minutes),
    )
    return bool(rows)


def record_alert(alert_key: str, severity: str, message: str) -> None:
    _execute(
        "INSERT INTO monitoring.alerts (alert_key, severity, message) VALUES (%s, %s, %s)",
        (alert_key, severity, message),
    )
