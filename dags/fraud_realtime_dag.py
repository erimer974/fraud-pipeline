"""DAG 1 — Détection de fraude en temps réel (toutes les minutes).

create_tables → fetch_transactions → detect_fraud ─┬─ send_notification ─┬→ store_in_db → record_metrics
                                                   └─ no_fraud ──────────┘

- fetch_transactions : appelle l'API, ne garde que 7 colonnes sur 21, contrôle la qualité.
  Une transaction invalide part en quarantaine et le run se termine proprement (statut
  « rejected ») au lieu d'échouer.
- detect_fraud : calcule la probabilité de fraude et aiguille vers l'alerte ou non.
- store_in_db : upsert sur trans_num — un rejeu ne crée jamais de doublon.
- record_metrics : écrit les métriques du run (durée, latence API, volume…) pour le
  monitoring, puis fait échouer le run si une tâche a échoué.
"""
import json
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from fraud_pipeline import model, monitoring, quality, storage
from fraud_pipeline.alerts import notify_failure, send_discord
from fraud_pipeline.config import (
    API_BASE,
    API_TIMEOUT_S,
    CHAMPS_CONSERVES,
    DATASET_PATH,
    POSTGRES_CONN_ID,
    get_setting,
    log,
)

default_args = {
    "owner": "airflow",
    # L'API publique peut être momentanément indisponible : nouvelles tentatives rapprochées
    # puis espacées (10 s puis 20 s), le tout tenant dans la minute du run suivant.
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=40),
    "on_failure_callback": notify_failure,   # alerte Discord (dédupliquée) si une tâche échoue
}


# ── Tâche 1 : Récupérer et contrôler la transaction en cours ──
def fetch_transactions(**context):
    ti = context["ti"]

    # Mode démo : rejoue une fraude connue du dataset (les fraudes réelles du flux sont
    # trop rares pour une démo en direct). Variable Airflow `demo_fraud` = "1" pour l'activer.
    if get_setting("demo_fraud") == "1":
        import pandas as pd

        df = pd.read_csv(DATASET_PATH, usecols=CHAMPS_CONSERVES + ["is_fraud"])
        row = df[df["is_fraud"] == 1].sort_values("amt", ascending=False).iloc[0]
        raw, source, latency_ms, columns_received = row[CHAMPS_CONSERVES].to_dict(), "demo", None, None
    else:
        start = time.perf_counter()
        response = requests.get(f"{API_BASE}/current-transactions", timeout=API_TIMEOUT_S)
        latency_ms = round((time.perf_counter() - start) * 1000, 1)
        response.raise_for_status()
        # Double décodage : l'API retourne une chaîne JSON dans une réponse JSON.
        data = json.loads(response.json())
        columns_received = len(data["columns"])
        # Filtrage à l'entrée : seules les colonnes de CHAMPS_CONSERVES franchissent la porte.
        # `is_fraud` (vérité terrain) et les données personnelles sont écartées ici.
        raw = {c: v for c, v in zip(data["columns"], data["data"][0], strict=False) if c in CHAMPS_CONSERVES}
        source = "api"

    ti.xcom_push(key="source", value=source)
    ti.xcom_push(key="api_latency_ms", value=latency_ms)

    errors = quality.validate_transaction(raw)
    if errors:
        # Donnée invalide : quarantaine + arrêt propre du run (pas d'échec, pas de prédiction).
        storage.insert_rejected([{"record": raw, "reasons": "; ".join(errors)}], source, context["run_id"])
        ti.xcom_push(key="rows_rejected", value=1)
        log.warning("[FETCH] Transaction rejetée par le contrôle qualité : %s", errors)
        raise AirflowSkipException(f"Transaction invalide mise en quarantaine : {errors}")

    transaction = quality.normalize_transaction(raw)
    ti.xcom_push(key="transaction", value=transaction)
    log.info(
        "[FETCH] %s — source=%s, %s colonnes reçues, %s conservées, latence=%s ms",
        transaction["trans_num"], source, columns_received, len(transaction), latency_ms,
    )


# ── Tâche 2 : Détecter la fraude avec le modèle local ─────────
def detect_fraud(**context):
    ti = context["ti"]
    t = ti.xcom_pull(key="transaction", task_ids="fetch_transactions")

    threshold = get_setting("fraud_threshold")
    proba = model.predict_proba([t])[0]
    prediction = int(proba >= threshold)

    # Clé "is_fraud_predicted" et non "is_fraud" : c'est la SORTIE DU MODÈLE, pas la vérité
    # terrain du dataset (qui, elle, n'entre jamais dans le système).
    ti.xcom_push(key="is_fraud_predicted", value=prediction)
    ti.xcom_push(key="fraud_proba", value=round(proba, 5))
    ti.xcom_push(key="model_version", value=model.model_version())
    log.info("[DETECT] proba=%.4f seuil=%s → prédiction=%s", proba, threshold, prediction)
    return "send_notification" if prediction == 1 else "no_fraud"


# ── Tâche 3a : Notification fraude ────────────────────────────
def send_notification(**context):
    ti = context["ti"]
    t = ti.xcom_pull(key="transaction", task_ids="fetch_transactions")
    proba = ti.xcom_pull(key="fraud_proba", task_ids="detect_fraud")
    sent = send_discord(
        f"**FRAUDE DÉTECTÉE**\n"
        f"Transaction : `{t['trans_num']}`\n"
        f"Montant : **{t['amt']} €**\n"
        f"Marchand : {t['merchant']}\n"
        f"Catégorie : {t['category']}\n"
        f"Probabilité : {proba:.0%}"
    )
    # La tâche ne doit pas échouer si Discord est indisponible, mais le log doit dire vrai.
    if sent:
        log.info("[DISCORD] Alerte envoyée pour %s", t["trans_num"])
    else:
        log.warning("[DISCORD] Alerte NON envoyée pour %s (voir message précédent)", t["trans_num"])


# ── Tâche 4 : Stocker en base (toujours exécutée) ─────────────
def store_in_db(**context):
    ti = context["ti"]
    t = ti.xcom_pull(key="transaction", task_ids="fetch_transactions")
    prediction = ti.xcom_pull(key="is_fraud_predicted", task_ids="detect_fraud")
    if not t or prediction is None:
        # trigger_rule="all_done" : on arrive ici même si l'amont a échoué ou a été ignoré
        # (transaction en quarantaine). Rien à stocker : arrêt propre.
        raise AirflowSkipException("Aucune transaction scorée à stocker.")

    rows = storage.upsert_transactions([{
        **t,
        "fraud_proba":        ti.xcom_pull(key="fraud_proba", task_ids="detect_fraud"),
        "is_fraud_predicted": prediction,
        "model_version":      ti.xcom_pull(key="model_version", task_ids="detect_fraud"),
        "source":             ti.xcom_pull(key="source", task_ids="fetch_transactions"),
        "dag_run_id":         context["run_id"],
    }])
    ti.xcom_push(key="rows_ok", value=rows)
    log.info("[DB] Upsert de %s ligne(s) pour %s", rows, t["trans_num"])


# ── Tâche 5 : Métriques du run (monitoring) ───────────────────
def record_metrics(**context):
    ti = context["ti"]
    pull = ti.xcom_pull
    rows_ok = pull(key="rows_ok", task_ids="store_in_db") or 0
    rows_rejected = pull(key="rows_rejected", task_ids="fetch_transactions") or 0
    monitoring.record_run_metrics(context, {
        "rows_in":        rows_ok + rows_rejected or None,
        "rows_ok":        rows_ok,
        "rows_rejected":  rows_rejected,
        "frauds":         pull(key="is_fraud_predicted", task_ids="detect_fraud") or 0,
        "api_latency_ms": pull(key="api_latency_ms", task_ids="fetch_transactions"),
        "model_version":  pull(key="model_version", task_ids="detect_fraud"),
        "fraud_proba":    pull(key="fraud_proba", task_ids="detect_fraud"),
        "source":         pull(key="source", task_ids="fetch_transactions"),
    })


# ── Définition du DAG ──────────────────────────────────────────
with DAG(
    dag_id="fraud_realtime",
    description="Détection de fraude toutes les minutes à partir de l'API de transactions",
    default_args=default_args,
    schedule="*/1 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,               # l'API ne sert que la transaction courante : rien à rattraper
    max_active_runs=1,           # un run lent ne se superpose pas au suivant
    dagrun_timeout=timedelta(minutes=5),
    tags=["fraude", "temps-réel"],
    doc_md=__doc__,
) as dag:

    t_create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/schema.sql",
    )
    t_fetch    = PythonOperator(task_id="fetch_transactions", python_callable=fetch_transactions)
    t_detect   = BranchPythonOperator(task_id="detect_fraud", python_callable=detect_fraud)
    t_notify   = PythonOperator(task_id="send_notification", python_callable=send_notification)
    t_no_fraud = EmptyOperator(task_id="no_fraud")
    t_store    = PythonOperator(
        task_id="store_in_db",
        python_callable=store_in_db,
        trigger_rule="all_done",   # s'exécute qu'il y ait fraude ou non
    )
    t_metrics  = PythonOperator(
        task_id="record_metrics",
        python_callable=record_metrics,
        trigger_rule="all_done",   # toujours exécutée, même après un échec
        retries=0,
        on_failure_callback=None,  # l'échec d'origine a déjà été alerté
    )

    t_create_tables >> t_fetch >> t_detect >> [t_notify, t_no_fraud] >> t_store >> t_metrics
