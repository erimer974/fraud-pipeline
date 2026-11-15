"""Traitements RGPD : droits des personnes, durée de conservation, sauvegardes.

Chaque opération est journalisée dans `audit.journal` (qui, quoi, quand, combien) : la
traçabilité des traitements est une preuve de conformité, pas un simple log technique.

L'identifiant d'une personne dans ce système est `trans_num` : grâce à la minimisation des
données à l'entrée, aucun nom, numéro de carte ni adresse n'est stocké.
"""
import gzip
import json
import logging
import os
import time
from datetime import UTC, datetime

from fraud_pipeline import storage
from fraud_pipeline.config import BACKUP_DIR, RGPD_EXPORT_DIR

log = logging.getLogger("airflow.task")

# Champs qu'une demande de rectification peut modifier. L'identifiant et les sorties du
# modèle ne sont pas rectifiables : ils résultent du traitement, pas d'une saisie.
RECTIFIABLE_FIELDS = {"merchant", "category", "amt", "city_pop", "merch_lat", "merch_long"}

BACKUP_TABLES = ["transactions", "rejected_transactions", "audit.journal", "monitoring.pipeline_runs"]


def export_personal_data(trans_num: str, reference: str, actor: str) -> str:
    """Droit d'accès (art. 15) : exporte en JSON toutes les données liées à `trans_num`."""
    transactions = storage.fetch_all(
        "SELECT row_to_json(t) FROM transactions t WHERE trans_num = %s", (trans_num,)
    )
    rejected = storage.fetch_all(
        "SELECT row_to_json(r) FROM rejected_transactions r WHERE trans_num = %s", (trans_num,)
    )
    export = {
        "reference_demande": reference,
        "trans_num": trans_num,
        "genere_le": datetime.now(UTC).isoformat(),
        "finalite": "Détection automatisée de la fraude bancaire",
        "duree_conservation": "voir docs/RGPD.md",
        "transactions": [r[0] for r in transactions],
        "transactions_rejetees": [r[0] for r in rejected],
    }
    os.makedirs(RGPD_EXPORT_DIR, exist_ok=True)
    path = f"{RGPD_EXPORT_DIR}/acces_{reference or trans_num}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2, default=str)
    storage.journal("rgpd_acces", actor, trans_num, {
        "reference": reference, "rows": len(transactions) + len(rejected), "export": path,
    })
    return path


def erase_personal_data(trans_num: str, reference: str, actor: str) -> int:
    """Droit à l'effacement (art. 17) : supprime toutes les lignes liées à `trans_num`."""
    deleted = storage._execute("DELETE FROM transactions WHERE trans_num = %s", (trans_num,))
    deleted += storage._execute("DELETE FROM rejected_transactions WHERE trans_num = %s", (trans_num,))
    storage.journal("rgpd_effacement", actor, trans_num, {"reference": reference, "deleted": deleted})
    return deleted


def rectify_personal_data(trans_num: str, changes: dict, reference: str, actor: str) -> int:
    """Droit de rectification (art. 16) : met à jour les champs autorisés."""
    forbidden = set(changes) - RECTIFIABLE_FIELDS
    if forbidden or not changes:
        reason = f"champs non rectifiables : {sorted(forbidden)}" if forbidden else "aucun champ à rectifier"
        # Un refus est aussi une réponse à la personne : il est tracé comme les autres.
        storage.journal("rgpd_rectification_refusee", actor, trans_num,
                        {"reference": reference, "motif": reason})
        raise ValueError(f"Rectification refusée : {reason}")
    assignments = ", ".join(f"{field} = %({field})s" for field in changes)
    updated = storage._execute(
        f"UPDATE transactions SET {assignments}, updated_at = now() WHERE trans_num = %(trans_num)s",
        {**changes, "trans_num": trans_num},
    )
    storage.journal("rgpd_rectification", actor, trans_num, {
        "reference": reference, "champs": sorted(changes), "updated": updated,
    })
    return updated


def purge_expired(retention_days: int, rejected_retention_days: int) -> dict:
    """Limitation de la conservation (art. 5-1-e) : supprime les données échues.

    La durée court depuis le dernier traitement (updated_at), pas depuis la première
    insertion : une transaction rescorée hier ne doit pas être purgée parce qu'elle avait
    été chargée il y a 90 jours.
    """
    deleted = storage._execute(
        "DELETE FROM transactions WHERE updated_at < now() - make_interval(days => %s)",
        (retention_days,),
    )
    rejected = storage._execute(
        "DELETE FROM rejected_transactions WHERE rejected_at < now() - make_interval(days => %s)",
        (rejected_retention_days,),
    )
    result = {"deleted": deleted + rejected, "transactions": deleted, "rejected": rejected,
              "retention_days": retention_days}
    storage.journal("purge_retention", "rgpd_maintenance", "transactions", result)
    return result


def purge_old_files(directories: list, max_age_days: int) -> int:
    """Supprime les fichiers (rapports, exports) plus anciens que `max_age_days`."""
    limit = time.time() - max_age_days * 86_400
    removed = 0
    for directory in directories:
        if not os.path.isdir(directory):
            continue
        for name in os.listdir(directory):
            path = os.path.join(directory, name)
            if os.path.isfile(path) and os.path.getmtime(path) < limit:
                os.remove(path)
                removed += 1
    storage.journal("purge_fichiers", "rgpd_maintenance", ",".join(directories),
                    {"removed": removed, "max_age_days": max_age_days})
    return removed


def backup_tables(day: str, keep: int) -> list:
    """Sauvegarde logique des tables (CSV compressé), avec rotation des plus anciennes."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    conn = storage.get_hook().get_conn()
    written = []
    try:
        with conn.cursor() as cur:
            for table in BACKUP_TABLES:
                path = f"{BACKUP_DIR}/{table.replace('.', '_')}_{day}.csv.gz"
                with gzip.open(path, "wt", encoding="utf-8") as f:
                    cur.copy_expert(f"COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER", f)
                written.append(path)
    finally:
        conn.close()

    # Rotation : ne garder que les `keep` dernières sauvegardes de chaque table.
    for table in BACKUP_TABLES:
        prefix = f"{table.replace('.', '_')}_"
        files = sorted(f for f in os.listdir(BACKUP_DIR) if f.startswith(prefix))
        for old in files[:-keep]:
            os.remove(os.path.join(BACKUP_DIR, old))

    storage.journal("sauvegarde", "rgpd_maintenance", ",".join(BACKUP_TABLES),
                    {"files": written, "kept": keep})
    return written
