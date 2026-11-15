"""Tests des métriques de run et des règles d'alerte du monitoring."""
import json
from datetime import UTC, datetime, timedelta

from fraud_pipeline.config import DEFAULTS
from fraud_pipeline.monitoring import build_run_record, evaluate_health
from fraud_pipeline.security import detect_volume_drop

NOW = datetime(2026, 10, 31, 12, 0, tzinfo=UTC)
THRESHOLDS = DEFAULTS["monitoring_thresholds"]
HEALTHY = {
    "runs": 30, "failed_runs": 0, "last_success_at": NOW - timedelta(minutes=1),
    "duration_p95_s": 12.0, "api_latency_ms": 400.0, "rows_ok": 30, "rows_rejected": 0,
    "frauds": 0, "daily_report_ok": True,
}


def keys(stats):
    return {a[0] for a in evaluate_health(stats, THRESHOLDS, NOW)}


def test_pipeline_sain_aucune_alerte():
    assert keys(HEALTHY) == set()


def test_pipeline_arrete():
    assert "health:freshness" in keys({**HEALTHY, "last_success_at": NOW - timedelta(minutes=20)})
    assert "health:freshness" in keys({**HEALTHY, "last_success_at": None})


def test_taux_echec():
    assert "health:error_rate" in keys({**HEALTHY, "failed_runs": 10})


def test_degradation_performances():
    assert "health:duration" in keys({**HEALTHY, "duration_p95_s": 90.0})
    assert "health:api_latency" in keys({**HEALTHY, "api_latency_ms": 15000.0})


def test_qualite_et_derive():
    assert "health:rejected_rate" in keys({**HEALTHY, "rows_rejected": 10})
    assert "health:fraud_rate" in keys({**HEALTHY, "frauds": 5})


def test_rapport_quotidien_manquant_seulement_apres_l_heure_limite():
    assert "health:daily_report" in keys({**HEALTHY, "daily_report_ok": False})
    early = evaluate_health({**HEALTHY, "daily_report_ok": False}, THRESHOLDS, NOW.replace(hour=7))
    assert "health:daily_report" not in {a[0] for a in early}


def test_record_statut_et_duree():
    start = NOW - timedelta(seconds=14.5)
    ok = build_run_record("fraud_realtime", "r1", NOW, start, NOW,
                          {"fetch_transactions": "success", "store_in_db": "success"},
                          {"rows_ok": 1, "frauds": 0, "fraud_proba": 0.02})
    assert ok["status"] == "success" and ok["duration_s"] == 14.5
    assert json.loads(ok["extra"]) == {"fraud_proba": 0.02}   # clé libre → colonne extra

    failed = build_run_record("fraud_realtime", "r2", NOW, start, NOW,
                              {"fetch_transactions": "failed", "detect_fraud": "upstream_failed"}, {})
    assert failed["status"] == "failed"
    assert failed["failed_tasks"] == "detect_fraud,fetch_transactions"

    rejected = build_run_record("fraud_realtime", "r3", NOW, start, NOW,
                                {"fetch_transactions": "skipped"}, {"rows_rejected": 1, "rows_ok": 0})
    assert rejected["status"] == "rejected"


def test_suppression_non_journalisee():
    assert detect_volume_drop(1000, 900, authorized_deleted=0) is True
    assert detect_volume_drop(1000, 900, authorized_deleted=100) is False   # purge légitime
    assert detect_volume_drop(1000, 1010, authorized_deleted=0) is False
    assert detect_volume_drop(None, 10, authorized_deleted=0) is False
