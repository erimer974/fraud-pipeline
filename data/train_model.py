"""Entraînement du modèle de détection de fraude (à lancer une fois, dans le conteneur worker).

    docker compose exec airflow-worker bash -c "cd /opt/airflow/data && python train_model.py"

Produit deux fichiers à côté du script :
- `fraud_model.pkl`  : le modèle sérialisé, chargé par les DAGs ;
- `model_card.json`  : sa fiche d'identité (version, date, variables, métriques, versions des
  librairies). La version est enregistrée avec chaque prédiction en base.
"""
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import joblib
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

HERE = Path(__file__).resolve().parent
DATASET = HERE / "fraudTest.csv"
MODEL_PATH = HERE / "fraud_model.pkl"
CARD_PATH = HERE / "model_card.json"

# unix_time retiré volontairement : c'est un timestamp absolu (la colonne couvre juin→déc. 2013,
# soit 1371816865 → 1388534374) qui rend le modèle aveugle en production — l'API de transactions
# tourne en 2026, donc toujours hors de la plage d'entraînement, même après conversion ms→s.
# On garde des features stables dans le temps.
FEATURES = ["amt", "city_pop", "merch_lat", "merch_long"]


def main():
    df = pd.read_csv(DATASET, usecols=FEATURES + ["is_fraud"])
    X, y = df[FEATURES], df["is_fraud"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # class_weight="balanced" : le jeu de données est très déséquilibré (~0,4 % de fraude).
    model = RandomForestClassifier(
        n_estimators=100, random_state=42, class_weight="balanced", n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Sur des données déséquilibrées, l'accuracy est trompeuse (99 % en prédisant « jamais
    # fraude »). On évalue donc recall / precision / F1 et la matrice de confusion.
    pred = model.predict(X_test)
    matrix = confusion_matrix(y_test, pred)
    print("Matrice de confusion [[tn, fp], [fn, tp]] :")
    print(matrix)
    print(classification_report(y_test, pred, digits=4))

    joblib.dump(model, MODEL_PATH)
    digest = hashlib.sha256(MODEL_PATH.read_bytes()).hexdigest()
    trained_at = datetime.now(UTC)
    card = {
        "version": f"rf-{trained_at:%Y%m%d%H%M}-{digest[:8]}",
        "trained_at": trained_at.isoformat(),
        "algorithm": "RandomForestClassifier(n_estimators=100, class_weight='balanced')",
        "features": FEATURES,
        "training_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "fraud_rate": round(float(y.mean()), 5),
        "metrics_test": {
            "recall": round(float(recall_score(y_test, pred)), 4),
            "precision": round(float(precision_score(y_test, pred)), 4),
            "f1": round(float(f1_score(y_test, pred)), 4),
            "confusion_matrix": matrix.tolist(),
        },
        "sklearn_version": sklearn.__version__,
        "sha256": digest,
    }
    CARD_PATH.write_text(json.dumps(card, indent=2), encoding="utf-8")
    print(f"Modèle sauvegardé dans {MODEL_PATH} — version {card['version']}")


if __name__ == "__main__":
    main()
