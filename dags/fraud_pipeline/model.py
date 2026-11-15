"""Chargement du modèle de détection et identification de sa version.

La version du modèle est enregistrée avec chaque prédiction (colonne `model_version`) : on
sait ainsi, pour toute transaction stockée, quel modèle l'a classée — traçabilité exigée dès
qu'une décision automatisée porte sur une personne.
"""
import hashlib
import json
import os
from functools import lru_cache

import pandas as pd

from fraud_pipeline.config import FEATURES, MODEL_CARD_PATH, MODEL_PATH


@lru_cache(maxsize=2)
def _load(path: str, mtime: float):
    import joblib

    return joblib.load(path)


def load_model(path: str = MODEL_PATH):
    """Charge le modèle, mis en cache tant que le fichier n'a pas changé sur le disque.

    La date de modification fait partie de la clé de cache : un réentraînement qui
    réécrit le .pkl est pris en compte au run suivant, sans redémarrer le worker.
    """
    return _load(path, os.path.getmtime(path))


def model_version(path: str = MODEL_PATH, card_path: str = MODEL_CARD_PATH) -> str:
    """Version lue dans la fiche modèle ; à défaut, empreinte SHA-256 du fichier."""
    if os.path.exists(card_path):
        with open(card_path, encoding="utf-8") as f:
            return json.load(f)["version"]
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return f"sha256-{digest.hexdigest()[:12]}"


def predict_proba(rows: list) -> list:
    """Probabilité de fraude pour une liste de transactions normalisées."""
    features = pd.DataFrame(rows)[FEATURES]
    return [float(p) for p in load_model().predict_proba(features)[:, 1]]
