FROM apache/airflow:2.10.5-python3.11
COPY requirements.txt requirements-dev.txt ./
# Dépendances d'exécution + outils de test/lint (pytest, ruff), pour lancer la suite de
# tests dans le même environnement que la production : `docker compose exec airflow-worker pytest`.
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt
