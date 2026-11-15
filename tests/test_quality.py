"""Tests du contrôle qualité des données entrantes."""
import pandas as pd
import pytest

from fraud_pipeline.quality import normalize_transaction, split_valid_rejected, validate_transaction

VALID = {
    "trans_num": "abc123", "amt": 42.5, "city_pop": 1200, "merchant": "fraud_Kirlin",
    "category": "shopping_net", "merch_lat": 40.1, "merch_long": -73.9,
}


def test_transaction_valide():
    assert validate_transaction(VALID) == []


@pytest.mark.parametrize("field", ["trans_num", "amt", "merchant", "merch_lat"])
def test_champ_manquant(field):
    t = {k: v for k, v in VALID.items() if k != field}
    assert any("manquants" in e and field in e for e in validate_transaction(t))


@pytest.mark.parametrize("field, value, fragment", [
    ("amt", -3, "montant"),
    ("amt", 5_000_000, "montant"),
    ("amt", "douze", "non numérique"),
    ("amt", float("nan"), "non numérique"),
    ("city_pop", -1, "population"),
    ("merch_lat", 95, "latitude"),
    ("merch_long", -200, "longitude"),
])
def test_valeurs_invalides(field, value, fragment):
    errors = validate_transaction({**VALID, field: value})
    assert any(fragment in e for e in errors)


def test_normalisation_types():
    t = normalize_transaction({**VALID, "amt": "42.567", "city_pop": "1200.0", "trans_num": 7})
    assert t == {**VALID, "amt": 42.57, "trans_num": "7"}
    assert isinstance(t["city_pop"], int)


def test_lot_separe_valides_et_rejetes_et_ecarte_les_donnees_personnelles():
    df = pd.DataFrame([
        {**VALID, "cc_num": 4111111111111111, "first": "Jane", "is_fraud": 1},
        {**VALID, "trans_num": "bad", "amt": -1, "cc_num": 4000, "first": "John", "is_fraud": 0},
    ])
    valid, rejected = split_valid_rejected(df)
    assert [v["trans_num"] for v in valid] == ["abc123"]
    assert rejected[0]["record"]["trans_num"] == "bad"
    # Ni la vérité terrain ni les données personnelles ne franchissent le contrôle.
    for record in valid + [r["record"] for r in rejected]:
        assert not {"cc_num", "first", "is_fraud"} & set(record)
