"""DAG 6 — Traitement des demandes d'exercice des droits RGPD (déclenchement manuel).

Interface Airflow → `rgpd_requests` → ▶ « Trigger DAG w/ config » : un formulaire demande le
type de demande, l'identifiant de transaction et la référence du dossier.

| Type           | Article RGPD | Effet                                                         |
|----------------|--------------|---------------------------------------------------------------|
| acces          | art. 15      | Export JSON de toutes les données liées → `data/rgpd_exports/` |
| rectification  | art. 16      | Mise à jour des champs fournis dans `modifications`            |
| effacement     | art. 17      | Suppression de toutes les lignes liées                         |

Chaque demande est tracée dans `audit.journal` (type, référence, auteur, volume concerné).
"""
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from fraud_pipeline import rgpd
from fraud_pipeline.alerts import notify_failure
from fraud_pipeline.config import log


def process_request(**context):
    p = context["params"]
    trans_num, reference = p["trans_num"].strip(), p["reference"].strip()
    # Personne qui traite la demande, consignée dans le journal d'audit. Airflow trace en
    # parallèle le compte qui a déclenché le run (Browse → Audit Logs).
    actor = p["traite_par"].strip()

    if p["type_demande"] == "acces":
        path = rgpd.export_personal_data(trans_num, reference, actor)
        log.info("[RGPD] Export d'accès généré : %s", path)
    elif p["type_demande"] == "rectification":
        updated = rgpd.rectify_personal_data(trans_num, p["modifications"], reference, actor)
        log.info("[RGPD] %s ligne(s) rectifiée(s) pour %s", updated, trans_num)
    else:
        deleted = rgpd.erase_personal_data(trans_num, reference, actor)
        log.info("[RGPD] %s ligne(s) effacée(s) pour %s", deleted, trans_num)


with DAG(
    dag_id="rgpd_requests",
    description="Droits des personnes : accès, rectification, effacement",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 0, "on_failure_callback": notify_failure},
    params={
        "type_demande": Param("acces", enum=["acces", "rectification", "effacement"],
                              description="Droit exercé par la personne concernée"),
        "trans_num": Param("", type="string", minLength=1,
                           description="Identifiant de la transaction concernée"),
        "reference": Param("", type="string", minLength=1,
                           description="Référence du dossier de demande (ex. RGPD-2026-001)"),
        "traite_par": Param("", type="string", minLength=1,
                            description="Nom de la personne qui traite la demande (journal d'audit)"),
        "modifications": Param({}, type="object",
                               description="Rectification uniquement : {\"champ\": nouvelle_valeur}"),
    },
    tags=["rgpd"],
    doc_md=__doc__,
) as dag:

    PythonOperator(task_id="process_request", python_callable=process_request)
