"""Configuration centrale du pipeline.

Trois natures de réglages, trois emplacements :
- les constantes de structure (chemins, colonnes) sont ici, versionnées avec le code ;
- les secrets arrivent par variables d'environnement (`.env` → docker-compose) ;
- les réglages métier modifiables à chaud sont des Variables Airflow, lues via `get_setting`.
"""
import json
import logging
import os

log = logging.getLogger("airflow.task")

# ── Source de données ─────────────────────────────────────────
API_BASE = "https://sdacelo-real-time-fraud-detection.hf.space"
API_TIMEOUT_S = 10

# ── Connexion à la base métier (définie par AIRFLOW_CONN_POSTGRES_FRAUD) ──
POSTGRES_CONN_ID = "postgres_fraud"

# ── Chemins (volume ./data monté dans les conteneurs) ─────────
DATA_DIR = "/opt/airflow/data"
DATASET_PATH = f"{DATA_DIR}/fraudTest.csv"
MODEL_PATH = f"{DATA_DIR}/fraud_model.pkl"
MODEL_CARD_PATH = f"{DATA_DIR}/model_card.json"
REPORTS_DIR = f"{DATA_DIR}/reports"
INBOX_DIR = f"{DATA_DIR}/inbox"            # dépôt de fichiers pour l'ingestion batch
ARCHIVE_DIR = f"{DATA_DIR}/archive"        # fichiers traités avec succès
ERROR_DIR = f"{DATA_DIR}/error"            # fichiers illisibles
BACKUP_DIR = f"{DATA_DIR}/backups"
RGPD_EXPORT_DIR = f"{DATA_DIR}/rgpd_exports"

# ── Données ───────────────────────────────────────────────────
# Les 7 seules colonnes autorisées à entrer dans le système (minimisation des données).
# L'API en renvoie 21, dont `is_fraud` (la vérité terrain) et des données personnelles
# (cc_num, first, last, street, dob, job, gender) : elles sont écartées dès l'entrée.
CHAMPS_CONSERVES = [
    "trans_num", "amt", "city_pop", "merchant", "category", "merch_lat", "merch_long",
]
# Variables du modèle : grandeurs stables dans le temps (pas de timestamp absolu).
FEATURES = ["amt", "city_pop", "merch_lat", "merch_long"]

# Formats acceptés par l'ingestion batch, et taille des lots (mémoire bornée quel que
# soit le volume du fichier).
BATCH_FORMATS = (".csv", ".json", ".jsonl", ".parquet")
BATCH_CHUNK_SIZE = 50_000

# ── Réglages modifiables à chaud (Variables Airflow) ──────────
DEFAULTS = {
    "fraud_threshold": 0.5,     # seuil appliqué à predict_proba
    "demo_fraud": "0",          # "1" : rejoue une fraude connue au lieu d'appeler l'API
    "retention_days": 90,       # durée de conservation des transactions (RGPD)
    "rejected_retention_days": 30,
    "files_retention_days": 30,  # rapports, exports RGPD, fichiers source archivés ou en erreur
    "backups_kept": 7,
    "monitoring_thresholds": {
        "window_min": 30,          # fenêtre d'observation des runs temps réel
        "freshness_min": 5,        # aucun run réussi depuis N min → pipeline arrêté
        "error_rate_pct": 20,      # part de runs en échec sur la fenêtre
        "duration_p95_s": 45,      # durée d'un run (p95)
        "api_latency_ms": 10000,   # latence moyenne de l'API (observée : 6-7 s)
        "fraud_rate_pct": 5,       # taux de fraude prédit anormalement haut (dérive)
        "rejected_rate_pct": 10,   # part de transactions rejetées par le contrôle qualité
        "daily_report_hour_utc": 9,  # heure après laquelle le rapport de la veille doit exister
        "realert_min": 60,         # une même alerte n'est renvoyée qu'après ce délai
    },
}


def get_setting(name):
    """Lit une Variable Airflow, avec la valeur par défaut de `DEFAULTS`.

    Les dictionnaires sont stockés en JSON ; une Variable partielle complète les valeurs
    par défaut au lieu de les remplacer (on peut ne surcharger qu'un seuil).
    """
    from airflow.models import Variable

    default = DEFAULTS[name]
    if isinstance(default, dict):
        raw = Variable.get(name, default_var=None)
        overrides = json.loads(raw) if raw else {}
        return {**default, **overrides}
    value = Variable.get(name, default_var=str(default))
    return type(default)(value)


def security_allowed_roles():
    """Rôles PostgreSQL légitimes sur la base métier (tout autre rôle est suspect)."""
    roles = os.environ.get("SECURITY_ALLOWED_DB_ROLES", "")
    return {r.strip() for r in roles.split(",") if r.strip()}
