# Conformité RGPD et sécurité des données

> Contexte : le projet traite un dataset **synthétique** (Kaggle). Ce document décrit le dispositif
> tel qu'il s'appliquerait à des transactions réelles ; les mécanismes techniques sont implémentés
> et testés, les éléments organisationnels (DPO, CNIL) sont décrits comme procédures.

---

## 1. Fiche du registre des traitements (art. 30)

| Rubrique | Contenu |
|---|---|
| Traitement | Détection automatisée de la fraude sur les transactions par carte |
| Finalité | Signaler les transactions suspectes en temps réel ; bilan quotidien |
| Base légale | **Intérêt légitime** de l'établissement (prévention de la fraude, considérant 47) et obligations de lutte contre la fraude. Le traitement **ne repose pas sur le consentement** |
| Personnes concernées | Porteurs de carte à l'origine des transactions |
| Données traitées | `trans_num` (identifiant de transaction), montant, marchand, catégorie, coordonnées du marchand, population de la ville du porteur |
| Données exclues dès l'entrée | numéro de carte, nom, prénom, adresse, date de naissance, profession, sexe, horodatage, vérité terrain `is_fraud` |
| Destinataires | Équipe fraude (alertes Discord : identifiant, montant, marchand, catégorie, probabilité) |
| Durée de conservation | Transactions : 90 jours après le dernier traitement (`updated_at`) · quarantaine : 30 jours · rapports et exports : 30 jours · sauvegardes : 7 jours · journal d'audit : conservé (preuve de conformité) |
| Décision automatisée (art. 22) | La prédiction **déclenche une alerte**, elle ne bloque rien : la décision finale reste humaine |
| Sécurité | Voir § 3 |

## 2. Minimisation (art. 5-1-c)

L'API renvoie 21 colonnes. `fetch_transactions` ne laisse passer que les 7 de `CHAMPS_CONSERVES`
(`dags/fraud_pipeline/config.py`) ; l'ingestion batch ne conserve que ces colonnes (pour un CSV,
seules elles sont lues : `usecols` ; pour JSON et Parquet, les autres sont écartées en mémoire avant
tout traitement). Les autres n'existent nulle part : ni en XCom, ni dans l'interface Airflow, ni en base,
ni dans les logs. Un test (`test_lot_separe_valides_et_rejetes_et_ecarte_les_donnees_personnelles`)
vérifie qu'un numéro de carte ou un nom présent dans un fichier ne franchit pas le contrôle.

Seules 4 de ces 7 colonnes servent au modèle ; les 3 autres servent à l'alerte et à l'identification
de la transaction (nécessaire pour exercer les droits).

## 3. Sécurité (art. 32)

| Mesure | Mise en œuvre |
|---|---|
| Secrets hors du code | `.env` non versionné (`.gitignore`), modèle `.env.example` ; aucune valeur de repli en dur ; démarrage refusé si un secret manque |
| Chiffrement des secrets Airflow | Clé **Fernet** obligatoire : mots de passe des connexions et Variables chiffrés dans la base de métadonnées |
| Mots de passe en base | Hachage **SCRAM-SHA-256** (PostgreSQL 16) ; mot de passe jamais écrit dans les logs (journalisation coupée lors de la création du rôle) |
| Moindre privilège | Airflow : seul rôle en écriture. Grafana : rôle `grafana_reader`, lecture du seul schéma `monitoring` (vérifié : accès refusé aux transactions et au journal) |
| Exposition réseau | Ports publiés sur `127.0.0.1` uniquement ; Redis et base de métadonnées non publiés |
| Comptes par défaut | Mot de passe de l'interface Airflow et de Grafana définis dans `.env` ; inscription Grafana désactivée |
| Disponibilité | Sauvegarde quotidienne, rotation 7 jours ; redémarrage automatique des services |
| **Non couvert** (à ajouter avant exposition) | TLS entre services, chiffrement du volume au repos, gestionnaire de secrets (Vault) |

Rotation de la clé Fernet : voir README § Dépannage.

## 4. Droits des personnes (art. 15 à 22)

DAG `rgpd_requests`, déclenché depuis l'interface Airflow (▶ *Trigger DAG w/ config*). Le formulaire
demande le type, le `trans_num`, la référence du dossier et la personne qui traite.

| Droit | Traitement | Résultat |
|---|---|---|
| Accès (art. 15) | `acces` | Export JSON de toutes les données liées, dans `data/rgpd_exports/` (finalité et durée incluses) |
| Rectification (art. 16) | `rectification` + `modifications` | Mise à jour des champs autorisés ; les sorties du modèle ne sont pas rectifiables (refus tracé) |
| Effacement (art. 17) | `effacement` | Suppression dans `transactions` et `rejected_transactions` |
| Limitation, opposition (art. 18, 21) | procédure manuelle | Traitée par effacement ou exclusion après examen ; la base légale (intérêt légitime) permet de motiver un refus pour la prévention de la fraude |

Procédure : réception de la demande (vérification d'identité hors système) → référence de dossier →
déclenchement du DAG → réponse à la personne sous **un mois**. Chaque demande est tracée dans
`audit.journal` (type, auteur, référence, volume).

**Consentement** : non applicable (base légale = intérêt légitime). Si une finalité secondaire
fondée sur le consentement était ajoutée (ex. réutilisation pour de la prospection), elle
nécessiterait une table `consentements` consultée par le filtrage à l'entrée ; ce n'est pas le cas.

**Conservation** : DAG `rgpd_maintenance`, chaque nuit — purge des transactions au-delà de
`retention_days` (90 j), de la quarantaine (30 j), des fichiers (30 j), journalisée avec le nombre de
lignes supprimées.

## 5. Traçabilité (art. 5-2, responsabilité)

| Question | Où trouver la réponse |
|---|---|
| D'où vient cette donnée ? | `transactions.source` (`api`, `demo`, `batch:<fichier>`) ; fichiers source archivés dans `data/archive/` |
| Quel traitement l'a produite ? | `transactions.dag_run_id` → run et logs Airflow ; `monitoring.pipeline_runs` |
| Quel modèle a décidé ? | `transactions.model_version` → `data/model_card.json` (date, variables, métriques, empreinte) |
| Quand ? | `recorded_at`, `updated_at` |
| Qui a exercé un droit, purgé, sauvegardé ? | `audit.journal` (`event_type`, `actor`, `details`, `at`) |
| Qui a accédé à la base ? | Logs PostgreSQL de `fraud-db` (`log_connections`, `log_disconnections`, `log_statement=ddl`) ; journal d'audit de l'interface Airflow (Browse → Audit Logs) |
| Pourquoi une donnée a été écartée ? | `rejected_transactions.reasons` |

## 6. Violations de données (art. 33-34)

**Détection automatique** (`pipeline_monitoring` → `security.py`, toutes les 10 min) :

| Signal | Exemple |
|---|---|
| Rôle PostgreSQL non autorisé | compte créé hors procédure |
| Session ouverte par un rôle non autorisé | accès en cours |
| Disparition de transactions non expliquée par une purge ou un effacement journalisé | suppression massive, corruption |

Chaque incident est **journalisé** (`audit.journal`, `event_type = incident_securite`) et **alerté**
sur Discord (🚨 SÉCURITÉ). Test réalisé : création d'un rôle `intrus` et suppression de 50 lignes →
deux alertes et deux entrées de journal en moins de 10 minutes.

**Procédure de gestion :**

1. **Contenir** (immédiat) : révoquer le rôle ou la session (`DROP ROLE`, `pg_terminate_backend`),
   changer les mots de passe concernés dans `.env`, mettre les DAGs en pause si nécessaire.
2. **Qualifier** (sous 24 h) : nature, données et nombre de personnes concernées, à partir de
   `audit.journal`, des logs PostgreSQL et de la dernière sauvegarde.
3. **Notifier la CNIL sous 72 h** si la violation présente un risque pour les personnes (art. 33) ;
   informer les personnes si le risque est élevé (art. 34).
4. **Restaurer** depuis `data/backups/` si des données ont été perdues.
5. **Documenter** l'incident dans le registre des violations (le journal d'audit en constitue la
   base), y compris s'il n'est pas notifié.
