# Monitoring du pipeline

Le monitoring répond à trois questions, pour trois publics :

| Question | Outil | Public |
|---|---|---|
| Le pipeline fonctionne-t-il **maintenant** ? | Tableau de bord Grafana, rafraîchi toutes les 30 s | équipe data, responsable métier |
| Faut-il **intervenir** ? | Alertes Discord (règles évaluées toutes les 10 min) | astreinte |
| Que s'est-il passé, **pourquoi** ? | Interface Airflow (Graph View, logs par tâche), tables `monitoring.*` | développeur |

---

## 1. Collecte : une ligne par run

Chaque DAG de traitement se termine par la tâche `record_metrics` (`trigger_rule="all_done"` : elle
s'exécute même après un échec). Elle écrit une ligne dans `monitoring.pipeline_runs` :

| Colonne | Contenu |
|---|---|
| `dag_id`, `run_id`, `logical_date` | identification du run |
| `started_at`, `ended_at`, `duration_s` | temps de traitement de bout en bout |
| `status`, `failed_tasks` | `success`, `rejected` (donnée mise en quarantaine) ou `failed`, avec les tâches en cause |
| `rows_in`, `rows_ok`, `rows_rejected` | volume lu, valide, rejeté par le contrôle qualité |
| `frauds` | fraudes prédites |
| `rows_per_s` | débit (batch et rapport) |
| `api_latency_ms` | temps de réponse de l'API source |
| `model_version` | version du modèle utilisée |
| `extra` (JSONB) | toute métrique supplémentaire : probabilité, source, fichiers en erreur… |

Les alertes émises sont historisées dans `monitoring.alerts`.

## 2. Visualisation : Grafana

http://localhost:3000 — tableau de bord « Pipeline de détection de fraude », chargé automatiquement
depuis `grafana/dashboards/fraud_pipeline.json` (versionné avec le code).

| Panneau | Ce qu'il montre |
|---|---|
| Minutes depuis le dernier run réussi | fraîcheur : vert < 3 min, rouge > 5 min |
| Transactions traitées / fraudes détectées | volumes sur la période |
| Taux d'échec des runs | tous DAGs confondus |
| Durée moyenne d'un run, latence de l'API | performance du temps réel |
| Durée des runs, latence de l'API (courbes) | tendance, dégradation progressive |
| Volume traité par heure | valides, rejetées, fraudes |
| Statut des runs par heure | succès, rejet, échec |
| Débit des traitements batch | lignes/s à chaque ingestion et rapport |
| Alertes émises | historique des alertes Discord |
| Derniers runs | détail : statut, tâches en échec, durée, modèle |

Grafana se connecte avec le rôle `grafana_reader`, qui ne peut **lire que le schéma `monitoring`** :
le tableau de bord n'a accès à aucune transaction ni au journal d'audit.

## 3. Alertes proactives

Le DAG `pipeline_monitoring` (toutes les 10 min) applique `monitoring.evaluate_health` :

| Règle | Seuil par défaut | Sévérité |
|---|---|---|
| Aucun run temps réel réussi depuis N min | 5 min | critique |
| Taux d'échec des runs sur la fenêtre (30 min) | 20 % | erreur |
| Durée des runs (p95) | 45 s | avertissement |
| Latence moyenne de l'API | 10 000 ms | avertissement |
| Taux de transactions rejetées | 10 % | avertissement |
| Taux de fraude prédit (dérive du modèle ou attaque) | 5 % | avertissement |
| Rapport quotidien de la veille absent après 9h UTC | — | erreur |

À cela s'ajoutent :
- l'alerte **ÉCHEC PIPELINE** immédiate sur toute tâche en échec (`on_failure_callback`) ;
- les alertes **SÉCURITÉ** (voir `RGPD.md`).

**Déduplication** : une même alerte n'est renvoyée qu'après `realert_min` (60 min ; 30 min pour les
échecs de tâche). Un échec qui se répète chaque minute ne sature pas le canal.

Le DAG de surveillance n'a aucune dépendance Airflow avec les DAGs surveillés : il continue
d'alerter même s'ils sont bloqués.

## 4. Adapter le monitoring

| Besoin | Modification |
|---|---|
| Changer un seuil | Variable Airflow `monitoring_thresholds`, ex. `{"duration_p95_s": 60}` — effet au run suivant |
| Suivre une nouvelle métrique | L'ajouter au dictionnaire passé à `record_run_metrics` : elle est stockée dans `extra`, sans migration de schéma |
| Afficher la métrique | Nouveau panneau Grafana : `SELECT ended_at AS time, (extra->>'ma_metrique')::numeric FROM monitoring.pipeline_runs …` |
| Nouvelle règle d'alerte | Une condition dans `evaluate_health` + un seuil dans `DEFAULTS["monitoring_thresholds"]` + un test dans `tests/test_monitoring.py` |
| Autre canal d'alerte | Remplacer `send_discord` dans `alerts.py` (Slack, Teams, e-mail) : tous les DAGs en bénéficient |

Extension possible vers une supervision d'infrastructure : activer StatsD dans Airflow
(`AIRFLOW__METRICS__STATSD_ON`) vers Prometheus, affiché dans le même Grafana.

## 5. Mesures relevées (31/10/2026)

| Mesure | Valeur |
|---|---|
| Durée d'un run temps réel | 17 à 19 s |
| Latence de l'API | 6,5 à 7 s |
| Ingestion de 555 719 lignes | 36,7 s (15 150 lignes/s) |
| Rapport quotidien | ~5 s |

Tests de bout en bout effectués : modèle retiré → run en échec, alerte avec l'erreur exacte, run
suivant rétabli ; rôle PostgreSQL non autorisé et suppression de 50 lignes → deux alertes SÉCURITÉ
journalisées ; dégradation de durée → alerte « durée dégradée ».
