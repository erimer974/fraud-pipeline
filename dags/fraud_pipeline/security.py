"""Détection d'incidents de sécurité sur la base métier (RGPD, art. 33-34).

Trois signaux, vérifiés par le DAG `pipeline_monitoring` :
1. un rôle PostgreSQL inconnu existe (compte créé hors procédure) ;
2. une session est ouverte par un rôle inconnu (accès non autorisé en cours) ;
3. le nombre de transactions chute sans purge ni effacement journalisé (suppression massive
   non autorisée ou corruption).

Chaque incident est journalisé dans `audit.journal` et alerté sur Discord (SÉCURITÉ). La
procédure de gestion (qualification, notification CNIL sous 72 h, registre des violations)
est décrite dans `docs/RGPD.md`.
"""
import logging

log = logging.getLogger("airflow.task")

UNKNOWN_ROLES_SQL = """
    SELECT rolname FROM pg_roles
    WHERE rolname NOT LIKE 'pg\\_%%' AND NOT (rolname = ANY(%s))
"""

UNKNOWN_SESSIONS_SQL = """
    SELECT usename, client_addr::text, count(*) FROM pg_stat_activity
    WHERE usename IS NOT NULL AND NOT (usename = ANY(%s))
    GROUP BY usename, client_addr
"""

# Purges et effacements légitimes depuis la dernière mesure.
AUTHORIZED_DELETIONS_SQL = """
    SELECT coalesce(sum((details->>'deleted')::int), 0) FROM audit.journal
    WHERE event_type IN ('purge_retention', 'rgpd_effacement') AND at > %s
"""


def detect_volume_drop(previous_count, current_count, authorized_deleted, tolerance=5):
    """Vrai si des lignes ont disparu au-delà des suppressions journalisées (fonction pure)."""
    if previous_count is None:
        return False
    unexplained = previous_count - current_count - authorized_deleted
    return unexplained > tolerance


def run_security_checks(allowed_roles: set) -> list:
    """Exécute les contrôles. Renvoie [(clé, message), ...] des incidents détectés."""
    from fraud_pipeline import storage

    incidents = []
    allowed = sorted(allowed_roles)

    for (role,) in storage.fetch_all(UNKNOWN_ROLES_SQL, (allowed,)):
        incidents.append((f"security:role:{role}", f"Rôle PostgreSQL non autorisé présent : `{role}`."))

    for user, addr, n in storage.fetch_all(UNKNOWN_SESSIONS_SQL, (allowed,)):
        incidents.append((
            f"security:session:{user}",
            f"{n} session(s) ouverte(s) par le rôle non autorisé `{user}` depuis {addr}.",
        ))

    # Volume : comparaison avec la mesure précédente, corrigée des suppressions légitimes.
    current = storage.fetch_all("SELECT count(*) FROM transactions")[0][0]
    previous = storage.fetch_all(
        "SELECT row_count, measured_at FROM monitoring.table_snapshots "
        "WHERE table_name = 'transactions' ORDER BY measured_at DESC LIMIT 1"
    )
    if previous:
        prev_count, prev_at = previous[0]
        authorized = storage.fetch_all(AUTHORIZED_DELETIONS_SQL, (prev_at,))[0][0]
        if detect_volume_drop(prev_count, current, authorized):
            incidents.append((
                "security:volume_drop",
                f"{prev_count - current - authorized} transaction(s) disparue(s) depuis "
                f"{prev_at:%Y-%m-%d %H:%M} sans purge ni effacement journalisé "
                f"({prev_count} → {current}).",
            ))
    storage._execute(
        "INSERT INTO monitoring.table_snapshots (table_name, row_count) VALUES ('transactions', %s)",
        (current,),
    )
    return incidents
