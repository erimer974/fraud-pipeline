"""Bibliothèque partagée par les DAGs du pipeline de détection de fraude.

Les DAGs (dans `dags/`) ne décrivent que l'enchaînement des tâches ; la logique métier vit ici,
dans des modules courts et testables séparément :

- `config`     : constantes et réglages (un seul endroit à modifier) ;
- `alerts`     : envoi des alertes Discord (métier, technique, sécurité) ;
- `quality`    : contrôles de qualité des données entrantes ;
- `model`      : chargement et version du modèle de détection ;
- `storage`    : écritures en base (upsert, quarantaine, journal d'audit) ;
- `monitoring` : métriques d'exécution et règles d'alerte ;
- `security`   : contrôles de détection d'incidents de sécurité ;
- `rgpd`       : droits des personnes, rétention, sauvegardes.

Ce dossier est exclu du parsing des DAGs par `dags/.airflowignore`.
"""
