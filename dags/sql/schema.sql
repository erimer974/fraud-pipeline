-- Schéma de la base métier fraud-db. Idempotent : exécuté au début de chaque run.
--
-- Trois espaces, trois publics :
--   public     : données métier (transactions)            → Airflow uniquement
--   monitoring : métriques et alertes du pipeline         → Airflow + Grafana (lecture seule)
--   audit      : journal des traitements et incidents     → Airflow uniquement

-- ── Données métier ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    trans_num          VARCHAR PRIMARY KEY,         -- clé d'unicité : upsert, aucun doublon
    amt                NUMERIC(12, 2) NOT NULL,
    merchant           VARCHAR NOT NULL,
    category           VARCHAR NOT NULL,
    merch_lat          NUMERIC(10, 6) NOT NULL,
    merch_long         NUMERIC(10, 6) NOT NULL,
    city_pop           INTEGER NOT NULL,
    fraud_proba        NUMERIC(6, 5),
    is_fraud_predicted SMALLINT NOT NULL,
    model_version      VARCHAR,                     -- quel modèle a pris la décision
    source             VARCHAR NOT NULL,            -- api | demo | batch:<fichier>
    dag_run_id         VARCHAR,                     -- quel run a traité la ligne
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_transactions_recorded_at ON transactions (recorded_at);
-- Rapport quotidien et purge de rétention portent sur la dernière décision (updated_at).
CREATE INDEX IF NOT EXISTS ix_transactions_updated_at ON transactions (updated_at);

-- Quarantaine : données invalides écartées par le contrôle qualité, avec la raison.
CREATE TABLE IF NOT EXISTS rejected_transactions (
    id          BIGSERIAL PRIMARY KEY,
    trans_num   VARCHAR,
    payload     JSONB,                              -- uniquement les 7 champs autorisés
    reasons     TEXT NOT NULL,
    source      VARCHAR NOT NULL,
    dag_run_id  VARCHAR,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_trans_num ON rejected_transactions (trans_num);

-- ── Monitoring ────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    id             BIGSERIAL PRIMARY KEY,
    dag_id         VARCHAR NOT NULL,
    run_id         VARCHAR NOT NULL,
    logical_date   TIMESTAMPTZ,
    started_at     TIMESTAMPTZ,
    ended_at       TIMESTAMPTZ NOT NULL,
    duration_s     NUMERIC(10, 3),
    status         VARCHAR NOT NULL,                -- success | rejected | failed
    failed_tasks   VARCHAR,
    rows_in        INTEGER,
    rows_ok        INTEGER,
    rows_rejected  INTEGER,
    frauds         INTEGER,
    rows_per_s     NUMERIC(12, 1),
    api_latency_ms NUMERIC(10, 1),
    model_version  VARCHAR,
    extra          JSONB,                           -- métriques additionnelles sans migration
    UNIQUE (dag_id, run_id)
);
CREATE INDEX IF NOT EXISTS ix_runs_dag_ended ON monitoring.pipeline_runs (dag_id, ended_at);

CREATE TABLE IF NOT EXISTS monitoring.alerts (
    id        BIGSERIAL PRIMARY KEY,
    alert_key VARCHAR NOT NULL,
    severity  VARCHAR NOT NULL,
    message   TEXT NOT NULL,
    raised_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_alerts_key_time ON monitoring.alerts (alert_key, raised_at);

CREATE TABLE IF NOT EXISTS monitoring.table_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    table_name  VARCHAR NOT NULL,
    row_count   BIGINT NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Audit ─────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.journal (
    id         BIGSERIAL PRIMARY KEY,
    event_type VARCHAR NOT NULL,   -- rgpd_acces | rgpd_effacement | purge_retention | incident_securite | ...
    actor      VARCHAR NOT NULL,   -- DAG ou utilisateur à l'origine du traitement
    target     VARCHAR,
    details    JSONB,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_journal_type_time ON audit.journal (event_type, at);
