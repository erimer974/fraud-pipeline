"""DAG 2 — Rapport quotidien des transactions et fraudes de la veille (chaque matin à 8h).

extract_yesterday → generate_report → record_metrics

- La date du rapport vient de `data_interval_start` (date logique) : le DAG est idempotent
  et rejouable — relancer le rapport du 27 juin produit bien celui du 27 juin.
- Le rapport compte les décisions du jour : filtre sur `updated_at` (dernier scoring). Limite :
  une transaction rescorée plus tard passe dans le rapport de ce jour-là ; rejouer un ancien
  rapport peut alors donner un total un peu plus faible.
- `catchup=True` : si la plateforme a été arrêtée plusieurs jours, les rapports manquants sont
  produits automatiquement au redémarrage, un par un (`max_active_runs=1`).
- Seul un petit résumé transite par XCom ; le détail est écrit sur disque.
"""
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from fraud_pipeline import monitoring, storage
from fraud_pipeline.alerts import notify_failure
from fraud_pipeline.config import REPORTS_DIR, log

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,   # alerte Discord si une tâche échoue
}


# ── Tâche 1 : Extraire les transactions de la veille ──────────
def extract_yesterday(**context):
    report_date = context["data_interval_start"].strftime("%Y-%m-%d")

    start = time.perf_counter()
    engine = storage.get_hook().get_sqlalchemy_engine()
    # Requête paramétrée (pas de date interpolée dans la chaîne SQL).
    # Filtre sur updated_at (date de la dernière décision du modèle) et non recorded_at (date
    # de la première insertion) : une transaction déjà en base, par exemple chargée par un banc
    # d'essai batch puis resservie par l'API, est mise à jour par l'upsert sans que recorded_at
    # bouge. Filtrer sur recorded_at la rendait invisible dans le rapport du jour.
    df = pd.read_sql(
        "SELECT * FROM transactions WHERE updated_at::date = %(d)s",
        engine,
        params={"d": report_date},
    )
    os.makedirs(REPORTS_DIR, exist_ok=True)
    detail_path = f"{REPORTS_DIR}/report_{report_date}.csv"
    df.to_csv(detail_path, index=False)
    elapsed = time.perf_counter() - start

    total = len(df)
    frauds = df[df["is_fraud_predicted"] == 1] if total else df
    summary = {
        "report_date": report_date,
        "total": total,
        "nb_fraudes": len(frauds),
        "montant_fraudes": round(float(frauds["amt"].sum()), 2) if len(frauds) else 0.0,
        "top_categories": {str(k): int(v) for k, v in frauds["category"].value_counts().head(5).items()},
        "par_source": {str(k): int(v) for k, v in df["source"].value_counts().items()} if total else {},
        "detail_path": detail_path,
        "extract_s": round(elapsed, 3),
        "rows_per_s": round(total / elapsed, 1) if elapsed and total else None,
    }
    context["ti"].xcom_push(key="summary", value=summary)
    log.info("[EXTRACT] %s transaction(s) pour le %s en %.2f s → %s",
             total, report_date, elapsed, detail_path)


# ── Tâche 2 : Générer le rapport ──────────────────────────────
def generate_report(**context):
    s = context["ti"].xcom_pull(key="summary", task_ids="extract_yesterday")
    total, nb_fraudes = s["total"], s["nb_fraudes"]
    taux = nb_fraudes / total * 100 if total else 0

    lines = [
        f"Rapport de fraude — {s['report_date']}",
        f"Transactions analysées : {total}",
        f"Fraudes détectées      : {nb_fraudes}",
        f"Taux de fraude         : {taux:.2f}%",
        f"Montant des fraudes    : {s['montant_fraudes']:.2f} €",
    ]
    if s["par_source"]:
        par_source = ", ".join(f"{k} = {v}" for k, v in s["par_source"].items())
        lines.append(f"Répartition par source : {par_source}")
    if s["top_categories"]:
        lines.append("Catégories les plus fraudées :")
        lines += [f"  - {cat} : {n}" for cat, n in s["top_categories"].items()]

    report = "\n".join(lines)
    log.info("[RAPPORT]\n%s", report)
    summary_path = f"{REPORTS_DIR}/summary_{s['report_date']}.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    log.info("[RAPPORT] Résumé sauvegardé : %s", summary_path)


# ── Tâche 3 : Métriques du run (monitoring) ───────────────────
def record_metrics(**context):
    s = context["ti"].xcom_pull(key="summary", task_ids="extract_yesterday") or {}
    monitoring.record_run_metrics(context, {
        "rows_in":     s.get("total"),
        "rows_ok":     s.get("total"),
        "frauds":      s.get("nb_fraudes"),
        "rows_per_s":  s.get("rows_per_s"),
        "report_date": s.get("report_date"),
        "extract_s":   s.get("extract_s"),
    })


# ── Définition du DAG ──────────────────────────────────────────
with DAG(
    dag_id="fraud_daily_report",
    description="Rapport quotidien des transactions et fraudes de la veille",
    default_args=default_args,
    schedule="0 8 * * *",
    # Début récent et borné : au premier démarrage, le rattrapage ne porte que sur
    # quelques jours. À avancer lors d'un nouveau déploiement.
    start_date=datetime(2026, 10, 25),
    catchup=True,
    max_active_runs=1,
    tags=["fraude", "batch"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(task_id="extract_yesterday", python_callable=extract_yesterday)
    t2 = PythonOperator(task_id="generate_report",   python_callable=generate_report)
    t3 = PythonOperator(
        task_id="record_metrics",
        python_callable=record_metrics,
        trigger_rule="all_done",
        retries=0,
        on_failure_callback=None,
    )

    t1 >> t2 >> t3
