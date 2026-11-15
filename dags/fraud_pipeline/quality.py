"""Contrôles de qualité des données entrantes.

Principe : une donnée invalide ne fait pas tomber le pipeline. Elle est écartée, placée en
quarantaine (table `rejected_transactions`) avec la raison du rejet, et le reste du flux
continue. Les fonctions de ce module sont pures (aucun accès réseau ni base) : elles sont
testées unitairement dans `tests/test_quality.py`.
"""
import math

import pandas as pd

from fraud_pipeline.config import CHAMPS_CONSERVES

# Bornes de plausibilité : au-delà, la valeur est une erreur de saisie ou de format,
# pas une transaction réelle.
AMT_MAX = 1_000_000


def _is_number(value) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return not math.isnan(float(value))
    except (TypeError, ValueError):
        return False


NUMERIC_FIELDS = ("amt", "city_pop", "merch_lat", "merch_long")


def _is_missing(field: str, value) -> bool:
    """Valeur absente : None ou chaîne vide ; pour un champ texte, aussi NaN — un champ vide d'un CSV
    arrive en NaN via pandas, et deviendrait sinon la chaîne « nan ». Un NaN numérique est signalé
    plus bas comme « non numérique »."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return True
    return field not in NUMERIC_FIELDS and isinstance(value, float) and math.isnan(value)


def validate_transaction(t: dict) -> list:
    """Renvoie la liste des anomalies d'une transaction (liste vide = transaction valide)."""
    errors = []
    missing = [c for c in CHAMPS_CONSERVES if _is_missing(c, t.get(c))]
    if missing:
        errors.append(f"champs manquants : {', '.join(missing)}")

    for field in NUMERIC_FIELDS:
        if field not in missing and not _is_number(t.get(field)):
            errors.append(f"{field} non numérique : {t.get(field)!r}")
    if errors:
        return errors

    if not 0 < float(t["amt"]) <= AMT_MAX:
        errors.append(f"montant hors bornes : {t['amt']}")
    if float(t["city_pop"]) < 0:
        errors.append(f"population négative : {t['city_pop']}")
    if not -90 <= float(t["merch_lat"]) <= 90:
        errors.append(f"latitude invalide : {t['merch_lat']}")
    if not -180 <= float(t["merch_long"]) <= 180:
        errors.append(f"longitude invalide : {t['merch_long']}")
    return errors


def normalize_transaction(t: dict) -> dict:
    """Ne garde que les champs autorisés et force les types attendus (à appeler après validation)."""
    return {
        "trans_num":  str(t["trans_num"]),
        "amt":        round(float(t["amt"]), 2),
        "city_pop":   int(float(t["city_pop"])),
        "merchant":   str(t["merchant"]),
        "category":   str(t["category"]),
        "merch_lat":  float(t["merch_lat"]),
        "merch_long": float(t["merch_long"]),
    }


def split_valid_rejected(df: pd.DataFrame):
    """Sépare un lot en (lignes valides normalisées, lignes rejetées avec leur raison).

    Les colonnes hors `CHAMPS_CONSERVES` sont écartées avant tout traitement : un fichier
    contenant des données personnelles superflues ne les fait pas entrer dans le système.
    """
    present = [c for c in CHAMPS_CONSERVES if c in df.columns]
    records = df[present].to_dict(orient="records")
    valid, rejected = [], []
    for record in records:
        errors = validate_transaction(record)
        if errors:
            rejected.append({"record": record, "reasons": "; ".join(errors)})
        else:
            valid.append(normalize_transaction(record))
    return valid, rejected
