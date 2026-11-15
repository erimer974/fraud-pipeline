#!/bin/bash
# Initialisation de fraud-db (exécuté une seule fois, à la création du volume).
#
# Principe du moindre privilège :
# - le rôle applicatif ($POSTGRES_USER) est utilisé par Airflow, seul à écrire ;
# - le rôle grafana_reader ne peut que LIRE le schéma `monitoring` (métriques du pipeline).
#   Il n'a aucun accès aux transactions ni au journal d'audit : le tableau de bord ne voit
#   jamais de données métier.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v grafana_password="$GRAFANA_DB_PASSWORD" <<'SQL'
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS audit;

-- log_statement=ddl journaliserait le mot de passe en clair : désactivé pour cette session.
SET log_statement = 'none';
CREATE ROLE grafana_reader LOGIN PASSWORD :'grafana_password';
RESET log_statement;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA monitoring TO grafana_reader;
-- Les tables du schéma monitoring sont créées plus tard par le pipeline : les droits de
-- lecture leur sont accordés automatiquement à leur création.
ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT SELECT ON TABLES TO grafana_reader;
SQL
