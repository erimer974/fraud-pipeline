"""Tests d'intégrité des DAGs : ils se chargent sans erreur et ont la structure attendue.

Ces tests nécessitent Airflow : ils tournent dans le conteneur worker et sont ignorés ailleurs.
"""
import os

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_aucune_erreur_d_import(dagbag):
    assert dagbag.import_errors == {}


def test_dags_attendus(dagbag):
    assert set(dagbag.dag_ids) == {
        "fraud_realtime", "fraud_daily_report", "fraud_batch_ingest",
        "pipeline_monitoring", "rgpd_maintenance", "rgpd_requests",
    }


def test_temps_reel_structure(dagbag):
    dag = dagbag.get_dag("fraud_realtime")
    assert dag.schedule_interval == "*/1 * * * *"
    assert dag.get_task("store_in_db").trigger_rule == "all_done"
    assert dag.get_task("record_metrics").trigger_rule == "all_done"
    assert dag.get_task("detect_fraud").downstream_task_ids == {"send_notification", "no_fraud"}


@pytest.mark.parametrize("dag_id", ["fraud_realtime", "fraud_daily_report", "fraud_batch_ingest"])
def test_chaque_traitement_enregistre_ses_metriques(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert "record_metrics" in dag.task_ids
    assert dag.get_task("record_metrics").downstream_task_ids == set()


def test_alerte_d_echec_sur_toutes_les_taches(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            if task.task_id != "record_metrics":
                assert task.on_failure_callback, f"{dag.dag_id}.{task.task_id} sans alerte d'échec"
