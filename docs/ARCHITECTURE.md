# Architecture — décisions et justifications

Ce document accompagne le schéma d'infrastructure ([`schema_infrastructure.svg`](schema_infrastructure.svg))
et répond à une seule question, pour chaque brique : **pourquoi celle-là, et pas une autre ?**

Le schéma dit *ce qui a été construit*. Ce document dit *pourquoi ça a été construit comme ça* : les
contraintes de départ, les options envisagées, celles écartées, et ce que chaque choix coûte.

---

## 1. L'infrastructure d'exécution

Le principe directeur tient en une phrase : **l'état doit être explicite, tout le reste doit être
jetable.** Aucune donnée durable ne vit dans un conteneur applicatif, rien n'est exposé sans
nécessité, et changer une donnée ou un modèle n'impose jamais de reconstruire une image.

### 1.1 Pourquoi Docker Compose

La reproductibilité est la raison évidente : la stack complète se lance en une commande. Mais
l'argument déterminant est spécifique à un projet de machine learning : **un modèle scikit-learn
sérialisé est lié à la version de la librairie qui l'a produit.** Un `.pkl` entraîné avec
scikit-learn 1.6 en local échoue au chargement dans un conteneur en 1.1 — ou, pire, se charge en
produisant des résultats silencieusement différents.

D'où la chaîne de garanties : `requirements.txt` épingle les versions (`scikit-learn==1.1.*`), le
`Dockerfile` construit l'image depuis `apache/airflow:2.10.5-python3.11` (sous un nom propre au
projet, `fraud-pipeline-airflow`, pour ne pas écraser localement le tag officiel), et **l'entraînement
s'exécute dans le conteneur worker**, pas sur l'hôte. Entraînement et inférence partagent donc le
même environnement par construction — sans conteneur, cette garantie ne repose que sur la discipline
de l'opérateur.

> `_PIP_ADDITIONAL_REQUIREMENTS`, proposé par le compose officiel, est volontairement laissé vide : il
> réinstalle les dépendances à chaque démarrage de chaque conteneur, ce qui allonge le démarrage sans
> rien figer. Les dépendances appartiennent à l'image, pas au runtime.

### 1.2 Pourquoi CeleryExecutor — et donc Redis

À la charge réelle de ce pipeline, `LocalExecutor` aurait fait le travail avec deux conteneurs de
moins. `CeleryExecutor` est retenu pour trois raisons :

1. **C'est la topologie de référence** du `docker-compose.yaml` officiel d'Airflow — celle qu'on
   retrouve en production.
2. **Elle sépare les rôles** : le scheduler planifie, le worker exécute. Un plantage de tâche ne
   dégrade pas le composant qui ordonnance toute la stack.
3. **Elle est la seule à offrir un chemin de montée en charge horizontal** : ajouter un worker revient
   à démarrer un conteneur de plus, sans réécrire un seul DAG. Le coût est payé une fois,
   l'évolutivité est acquise.

**Redis n'est pas un choix, c'est une conséquence** : Celery a besoin d'un broker pour transporter les
tâches du scheduler vers les workers. Il ne stocke aucune donnée métier — uniquement la file d'attente
— d'où le fait qu'il ne soit jamais exposé (§1.4). Le coût est assumé : deux conteneurs de plus
(`redis`, `airflow-worker`) et la mémoire correspondante.

> **`airflow-triggerer` et `flower`** viennent avec la configuration de référence. Le triggerer ne
> sert qu'aux tâches différées (*deferrable*), non utilisées ici, mais il est conservé pour ne pas
> diverger de la configuration officielle. `flower` est derrière le **profil `flower`** : il ne
> démarre pas avec un `docker compose up` simple. C'est la bonne valeur par défaut — une interface de
> supervision exposée en permanence, pour un seul worker, ne se justifie pas.

### 1.3 Pourquoi des volumes montés plutôt qu'une image auto-portante

| Monté | Pourquoi ce n'est pas dans l'image |
|---|---|
| `./dags` | Le scheduler relit le dossier en continu : un DAG corrigé est pris en compte sans rebuild ni redémarrage |
| `./data` | Contient le dataset (~150 Mo), le script d'entraînement, le modèle `.pkl` et les rapports. **Un réentraînement doit être disponible immédiatement** : le worker écrit le nouveau `.pkl`, le run suivant le charge |
| `./logs` | Les logs doivent **survivre à la destruction du conteneur** et rester lisibles depuis l'hôte — un log qui disparaît avec le conteneur qui a échoué est un log inutile |

À l'inverse, `requirements.txt` et le `Dockerfile` restent dans l'image : ils définissent
l'environnement d'exécution, doivent être figés, et toute modification **justifie** un rebuild.

> La règle appliquée : **l'image porte ce qui doit être identique partout ; le volume porte ce qui
> doit pouvoir changer sans reconstruction.**

Les dossiers `plugins/` et `config/` du compose officiel ont été commentés : le projet n'en utilise
aucun.

### 1.4 Pourquoi ces ports — et surtout ceux qui restent fermés

La question posée pour chaque service n'est pas « faut-il
l'ouvrir ? » mais « **qui, en dehors du réseau Docker, a besoin de le joindre ?** »

| Service | Publié ? | Raison |
|---|---|---|
| `airflow-webserver` | **127.0.0.1:8080** | L'interface est le poste de pilotage : DAGs, logs, connexions, variables |
| `grafana` | **127.0.0.1:3000** | Le tableau de bord de suivi, destiné aux parties prenantes |
| `fraud-db` | **127.0.0.1:5433** | Inutile au pipeline (les conteneurs la joignent en interne par `fraud-db:5432`), conservé pour inspecter la base depuis l'hôte pendant la démonstration |
| `flower` | **127.0.0.1:5555**, à la demande | Derrière un profil : lancé ponctuellement, pas en permanence |
| `redis` | **non** | Seuls le scheduler et le worker le contactent, à l'intérieur du réseau |
| `postgres` (métadonnées) | **non** | L'exposer offrirait un accès direct aux métadonnées — dont les connexions et variables Airflow |

Tous les ports publiés le sont sur la **boucle locale** (`127.0.0.1:…`) et non sur `0.0.0.0` : ils
sont joignables depuis le poste, pas depuis le réseau. Chaque port hôte est réglable dans `.env`
(`AIRFLOW_WEB_PORT`, `FRAUD_DB_PORT`, `GRAFANA_PORT`) en cas de conflit, sans toucher au compose.

**Pourquoi 5433 et non 5432** : le 5432 de l'hôte est souvent déjà pris par une installation
PostgreSQL locale. Le conteneur écoute bien sur 5432 en interne ; seule la correspondance côté hôte
change (`5433:5432`). Le même conflit peut survenir sur le 8080 ou le 3000 : la correspondance se
change dans `.env`, sans toucher au reste.

---

## 2. L'orchestration

### 2.1 Pourquoi Apache Airflow

Le besoin n'était pas de lancer du code périodiquement — un cron aurait suffi — mais de couvrir quatre
exigences qu'un simple planificateur n'offre pas :

- **des actions conditionnelles** : notifier *seulement si* une fraude est prédite, mais stocker dans
  tous les cas ;
- **des reprises en cas d'erreur** : l'API distante peut être indisponible (`retries`, `retry_delay`) ;
- **un historique consultable** : savoir *quel* run a échoué, *quand*, et lire ses logs ;
- **de la rejouabilité** : relancer un traitement passé et obtenir exactement le même résultat.

Airflow apporte ces quatre points nativement (Graph View, logs par tâche, `trigger_rule`,
`data_interval`). Un script Python (boucle `while` + `sleep`) n'offre ni historique, ni reprise, ni
visibilité — et un plantage l'arrête définitivement, en silence.

### 2.2 Pourquoi deux DAGs séparés

Dans Airflow, la
fréquence se définit au niveau du DAG, pas de la tâche : **une fréquence = un DAG**.

| | `fraud_realtime` | `fraud_daily_report` |
|---|---|---|
| Fréquence | `*/1 * * * *` | `0 8 * * *` |
| Source | API Hugging Face | Base PostgreSQL |
| Rôle | Détection immédiate | Bilan quotidien agrégé |

Le couplage se fait **par la donnée** (la table PostgreSQL), pas par des dépendances Airflow : le DAG
de rapport lit ce que le DAG temps réel a écrit. Chacun peut être arrêté, relancé ou modifié sans toucher
l'autre.

La même règle a guidé les quatre DAGs ajoutés ensuite, chacun avec son propre rythme :
`fraud_batch_ingest` (toutes les 5 min, sur dépôt de fichier), `pipeline_monitoring` (10 min),
`rgpd_maintenance` (chaque nuit) et `rgpd_requests` (à la demande). Aucun ne dépend d'un autre dans
Airflow ; tous se rejoignent dans `fraud-db`.

### 2.3 Les quatre choix internes aux DAGs

**`BranchPythonOperator` plutôt qu'un `if`.** Fonctionnellement identique ; la différence est visuelle.
Dans le Graph View, on voit immédiatement quelle branche a été empruntée après chaque run. Sur un
pipeline où 99,6 % des runs ne déclenchent aucune alerte, cette lisibilité permet de distinguer « le
pipeline fonctionne normalement » de « le pipeline est cassé », sans ouvrir un seul log.

**`trigger_rule="all_done"` sur le stockage.** Par défaut, une tâche ne s'exécute que si toutes ses
tâches amont ont réussi. Or `store_in_db` est en aval d'un branchement : une des deux branches est
nécessairement ignorée (*skipped*), et cet état se propagerait : le stockage serait ignoré à chaque
run. Cette règle lève la contrainte, ce qui est essentiel à l'objectif métier : **historiser toutes
les transactions**, fraudes comme non-fraudes.

**La date logique plutôt que `datetime.now()`.** Le DAG de rapport utilise `data_interval_start`. La
différence est cruciale en cas de rejeu : relancer aujourd'hui le rapport du 27 juin avec
`datetime.now()` produirait le rapport… du jour de la relance. Avec la date logique, il produit bien
celui du 27 juin. Le DAG est donc **idempotent**.

**XCom allégé dans le DAG de rapport.** XCom stocke ses valeurs dans la base de métadonnées. Y faire
transiter le DataFrame complet d'une journée la ferait grossir inutilement, pour une donnée déjà
écrite sur disque. Le DAG écrit le détail dans le CSV et ne transmet qu'un petit dictionnaire (total,
nombre de fraudes, top catégories, chemin du fichier). Dans le DAG temps réel, une transaction unique
est un objet minuscule, parfaitement adapté à XCom.

---

## 3. Le modèle de détection

### 3.1 Pourquoi un modèle local et non l'API

L'API Hugging Face **n'est pas un service de prédiction**. Elle joue un rôle unique : fournir une
transaction issue du même dataset Kaggle que `fraudTest.csv`. Le champ `is_fraud` qu'elle renvoie est
la **vérité terrain** du dataset.

Il faut donc entraîner notre propre algorithme sur `fraudTest.csv`, le sauvegarder en
`fraud_model.pkl`, et le charger dans le DAG au moment de la prédiction — soit exactement les deux
étapes attendues du bloc : *entraîner un algorithme* puis *le mettre en production*.

### 3.2 Pourquoi la vérité terrain est filtrée à l'entrée

L'API renvoie **21 colonnes**, dont `is_fraud`. Ne pas s'en servir pour prédire est nécessaire, mais
insuffisant : tant que la colonne circule dans le pipeline, la garantie repose sur la discipline du
code — « on ne la lit pas ». C'est difficile à démontrer et facile à casser lors d'une modification.

`fetch_transactions` applique donc un **filtrage à l'entrée** : seules les 7 colonnes réellement
utilisées franchissent la porte, listées dans la constante `CHAMPS_CONSERVES` — 4 pour la prédiction
(`amt`, `city_pop`, `merch_lat`, `merch_long`) et 3 pour l'alerte et le stockage (`trans_num`,
`merchant`, `category`).

La garantie change alors de nature : la vérité terrain **n'existe nulle part** dans le système — ni en
XCom, ni dans l'interface Airflow, ni en base. On ne démontre plus qu'on s'abstient de la lire ; on
constate son absence.

**Bénéfice second, tout aussi important** : les 14 colonnes écartées comprenaient `cc_num` (numéro de
carte bancaire), `first`, `last`, `street`, `dob`, `job` et `gender`. Ces données personnelles étaient
jusque-là persistées en clair dans la base de métadonnées d'Airflow et affichées dans son interface,
sans qu'aucune tâche ne les utilise. Le filtrage supprime cette collecte inutile — c'est le principe
de **minimisation des données**.

Un contrôle qualité accompagne le filtrage : si l'API cessait de renvoyer l'un des 7 champs attendus,
la transaction part en quarantaine (`rejected_transactions`, avec la raison) et le run se termine
sans prédiction (§7). Aucune prédiction n'est faite sur des champs manquants, et la hausse du taux
de rejet déclenche une alerte de monitoring.

### 3.3 Pourquoi l'entraînement est hors DAG

L'entraînement est une opération **one-shot**, lancée manuellement une fois. L'intégrer au DAG temps
réel ferait tourner un RandomForest sur 555 719 lignes **toutes les minutes**, pour reproduire à
chaque fois le même modèle — plusieurs minutes de calcul par run, pour un DAG qui doit s'exécuter en
quelques secondes.

Un réentraînement automatique aurait sa place dans un DAG dédié, déclenché périodiquement ou
sur dérive du modèle.

### 3.4 Pourquoi le temps a été retiré des variables

Cette décision vient d'un bug réel : le pipeline ne détectait **jamais** de fraude — 0 sur environ
6 000 transactions.

L'API renvoie `current_time` en **millisecondes** (ex. `1782558763368`), alors que la colonne
`unix_time` du dataset est en **secondes** et couvre juin à décembre 2013. Mais convertir ne corrige
rien : même converti, un timestamp 2026 (~1,78 × 10⁹) reste très au-delà du maximum appris
(1 388 534 374, fin 2013). Le problème n'est pas l'unité, c'est la nature de la variable.

**Un timestamp absolu est inutilisable en production** : par définition, la date de production est
toujours postérieure à la date d'entraînement, donc toujours hors distribution.

Solution : ne conserver que des grandeurs stables dans le temps — `amt` (montant), `city_pop`
(population), `merch_lat` et `merch_long` (coordonnées du commerçant). Le recall passe de ~0 % à ~30 %.

### 3.5 Pourquoi un RandomForest, et pourquoi ces métriques

`RandomForestClassifier` a été retenu pour trois raisons : il tolère des variables d'échelles très
différentes sans normalisation (un montant en euros et une latitude cohabitent sans traitement), il
capture des interactions non linéaires, et il expose `predict_proba` — ce qui rend le seuil de
décision réglable.

`class_weight="balanced"` compense le déséquilibre. Sans lui, le modèle minimise son erreur en
prédisant toujours « pas de fraude » : 99,6 % d'accuracy, et une inutilité parfaite.

C'est aussi pourquoi **l'accuracy n'est pas la métrique de suivi**. Les métriques suivies sont le
**recall** (30 % — quelle proportion de fraudes est détectée), la **precision** (72 % — quelle
proportion d'alertes est justifiée) et la matrice de confusion.

---

## 4. Le stockage

### 4.1 Pourquoi une base PostgreSQL séparée de celle d'Airflow

La stack comporte **deux** instances PostgreSQL, sans redondance :

| Instance | Port | Rôle |
|---|---|---|
| `postgres` (PostgreSQL 13) | non publié | **Métadonnées Airflow** : runs, états des tâches, XCom, connexions |
| `fraud-db` (PostgreSQL 16) | 127.0.0.1:5433 | **Base métier** : transactions, quarantaine, métriques du pipeline, journal d'audit |

`fraud-db` est découpée en trois schémas, un par public : `public` (données métier, Airflow
seul), `monitoring` (métriques et alertes, lues aussi par Grafana), `audit` (journal des traitements
et incidents, Airflow seul). Les droits se donnent alors par schéma, pas table par table.

PostgreSQL 16 plutôt que 13 pour `fraud-db` : les mots de passe y sont hachés en SCRAM-SHA-256 par
défaut. En version 13, le mot de passe initial est stocké en MD5 — essayé, l'authentification SCRAM
échouait.

Mélanger les deux serait risqué : toute opération de maintenance sur Airflow — purge de métadonnées,
migration de schéma lors d'une montée de version, réinitialisation — emporterait l'historique métier.
La séparation permet aussi d'arrêter l'un sans impacter l'autre.

PostgreSQL s'imposait par ailleurs : `PostgresHook` (provider officiel) et
`SQLExecuteQueryOperator` sont disponibles nativement dans Airflow — aucune brique d'intégration à
écrire.

### 4.2 Pourquoi des volumes nommés pour les deux bases

| | `postgres` (métadonnées) | `fraud-db` (métier) |
|---|---|---|
| Déclaration | Service du `docker-compose.yaml` | Service du `docker-compose.yaml` |
| Stockage | **Volume nommé** `postgres-db-volume` | **Volume nommé** `fraud-db-volume` |
| Exposition hôte | Aucune | 127.0.0.1:5433 |

**Pourquoi un volume nommé et non un bind mount pour les bases ?** Un répertoire PostgreSQL
exige des permissions POSIX strictes sur son `PGDATA`. Monté depuis un système de fichiers Windows, il
produit des erreurs de permissions ou des corruptions silencieuses. Un volume nommé reste géré par
Docker, dans son propre espace : c'est le seul mode fiable ici. À l'inverse, `data/` et `logs/` sont
faits pour être ouverts depuis l'hôte, d'où le bind mount. **La règle : bind mount pour ce
qu'un humain doit lire, volume nommé pour ce que seul un service doit écrire.**

### 4.3 Pourquoi `fraud-db` est dans le compose — une dette remboursée

**Dans la première version**, `fraud-db` était lancée par un `docker run` à part, sans volume, et
la connexion `postgres_fraud` était saisie à la main dans l'interface, pointant sur
`host.docker.internal:5433`. Raison historique : la base avait été créée pendant la mise au point,
en suivant la démarche du cours. Cela coûtait trois choses : une installation en trois étapes
manuelles, un trafic qui sortait vers l'hôte pour revenir, et une configuration ni versionnée ni
reproductible.

**La correction** :

- `fraud-db` est un service du compose, avec un **volume nommé** (`fraud-db-volume`) et un
  healthcheck dont dépendent les services Airflow ;
- au premier démarrage, le schéma (`dags/sql/schema.sql`) et le rôle Grafana en lecture seule
  (`fraud-db/init/01_roles.sh`) sont créés automatiquement ;
- la connexion est injectée par variable d'environnement :

```yaml
AIRFLOW_CONN_POSTGRES_FRAUD: postgresql://${FRAUD_DB_USER}:${FRAUD_DB_PASSWORD}@fraud-db:5432/${FRAUD_DB_NAME}
```

Les trois problèmes ont disparu ensemble : `docker compose up -d` suffit, le trafic reste interne
(`fraud-db:5432`), la configuration est versionnée (le mot de passe restant dans `.env`). **Aucun
DAG n'a été modifié pour cela** : `PostgresHook("postgres_fraud")` est resté identique, seule la
définition de la connexion a changé — la démonstration concrète du découplage recherché.

### 4.4 Pourquoi les rapports sont des fichiers et non une table

Il s'agit du rapport produit par `fraud_daily_report`.
Deux fichiers écrits dans `data/reports/` à chaque exécution : `report_<date>.csv` (le détail,
réexploitable dans un tableur) et `summary_<date>.txt` (le résumé lisible : total, nombre de fraudes,
taux, montant fraudé, répartition par source, top 5 des catégories).

Ces fichiers sont **destinés à être lus par un humain**, pas requêtés par un autre système. La donnée source, elle, reste en base — c'est
précisément ce que ce DAG interroge. Le dossier étant monté en volume, les fichiers sont directement
accessibles depuis l'hôte, sans client SQL.

---

## 5. La notification

**Pourquoi un webhook Discord.** Le sujet demande de pouvoir être notifié, et un `print()` dans les
logs ne notifie personne. L'`EmailOperator` d'Airflow impose d'administrer un serveur SMTP, pour un
résultat qui finit souvent en spam. Un webhook ne demande **aucune infrastructure** : une URL, une
requête POST, et le message arrive sur mobile comme sur desktop.

Le module `dags/fraud_pipeline/alerts.py` est partagé par tous les DAGs, ce qui évite de dupliquer
l'URL et la logique d'envoi. Trois familles d'alertes, préfixées pour être triées d'un coup d'œil :
**FRAUDE** (métier), **PIPELINE** (échec de tâche via `on_failure_callback`, ou règle de monitoring
franchie) et **SÉCURITÉ** (incident potentiel sur les données).

**Pourquoi une déduplication.** Dans la première version, une API en panne produisait une alerte
d'échec par minute : un canal saturé est un canal qu'on cesse de lire. Chaque alerte est désormais
enregistrée dans `monitoring.alerts`, et une même alerte (même DAG, même tâche, ou même règle)
n'est renvoyée qu'après un délai (30 min pour les échecs, 60 min pour les règles). Cette table sert
aussi d'historique dans Grafana. Si la base est indisponible, l'alerte part quand même : la
déduplication ne doit jamais empêcher d'alerter.

**Pourquoi elle ne peut jamais faire échouer une tâche.** L'envoi est enveloppé dans un `try/except`
qui n'échoue jamais. Si Discord est
indisponible, la transaction doit malgré tout être analysée et stockée. Laisser l'exception remonter
marquerait la tâche en échec et déclencherait une nouvelle tentative — qui échouerait aussi.

---

## 6. Configuration et secrets

Deux emplacements, et le critère de séparation est **le moment
où la valeur change** :

| | `.env` | Variables Airflow |
|---|---|---|
| Lu | au **démarrage du conteneur** | à **chaque exécution de tâche** |
| Contient | webhook Discord, clés Fernet et de session, mots de passe de `fraud-db` et de Grafana, comptes, ports | seuil de décision, mode démo, durées de conservation, seuils d'alerte |
| Pour changer la valeur | redémarrer la stack | rien — effet immédiat |
| Versionné ? | non (`.gitignore`), modèle dans `.env.example` | stocké en base de métadonnées |

Un secret d'infrastructure ne change jamais en fonctionnement : sa place est dans l'environnement du
conteneur. La chaîne est : `.env` non versionné → `docker-compose.yaml` relaie la variable → le code
la lit **sans valeur de repli en dur**. Ce dernier point est délibéré : une valeur de repli « pour le
développement » finit dans l'historique Git, où elle reste après suppression. Pour les secrets
indispensables, le compose utilise la syntaxe `${VARIABLE:?message}` : la stack **refuse de démarrer**
si la valeur manque, plutôt que de démarrer avec un secret vide.

**La clé Fernet.** Dans la première version, `AIRFLOW__CORE__FERNET_KEY` était vide — le réglage par
défaut du compose officiel. Conséquence : les mots de passe des connexions et les Variables étaient
stockés **en clair** dans la base de métadonnées. La clé est désormais obligatoire et vit dans
`.env`.

Un réglage métier, lui, doit pouvoir être ajusté **pendant** une démonstration :

| Variable | Défaut | Rôle |
|---|---|---|
| `fraud_threshold` | `0.5` | Seuil appliqué à `predict_proba` — l'abaisser capte plus de fraudes au prix de plus de faux positifs |
| `demo_fraud` | `0` | Si `1`, rejoue une fraude connue au lieu d'appeler l'API |

`demo_fraud` répond à un problème de démontrabilité : avec 0,39 % de fraudes et une transaction par
minute, une vraie fraude n'apparaît qu'environ toutes les 4 heures. Le mode démo permet d'en montrer
une à la demande, **sans truquer ni le modèle ni le seuil**.

Les autres Variables (`retention_days`, `monitoring_thresholds`…) suivent la même logique ; leurs
valeurs par défaut sont centralisées dans `dags/fraud_pipeline/config.py`.

---

## 7. La qualité des données et la reprise sur erreur

**Pourquoi une quarantaine plutôt qu'un échec.** Une transaction mal formée (montant négatif,
coordonnées impossibles, champ manquant) n'est pas une panne du pipeline : c'est une donnée à
écarter. La faire échouer déclencherait des nouvelles tentatives inutiles et une alerte technique.
`quality.validate_transaction` la place dans `rejected_transactions` avec la raison du rejet, et le
run se termine avec le statut `rejected`. Le flux continue ; le taux de rejet est surveillé (une
hausse signale un changement de format côté source).

**Pourquoi un upsert.** La table `transactions` a `trans_num` pour clé primaire, et toute écriture
est un `INSERT … ON CONFLICT DO UPDATE`. Une nouvelle tentative, un run relancé à la main, le mode démo
rejoué : aucun ne crée de doublon. C'est ce qui rend la reprise automatique sûre — on peut toujours
réexécuter.

L'upsert met à jour la décision (`fraud_proba`, `is_fraud_predicted`, `model_version`), la
`source`, le `dag_run_id` et `updated_at` ; `recorded_at` garde la date de première insertion. Le
rapport quotidien et la purge de rétention portent donc sur `updated_at`, la date de la dernière
décision. Cette règle vient d'un défaut constaté le 11/11 : après le chargement complet de
`fraudTest.csv` par le banc d'essai batch (31/10), chaque transaction de l'API — tirée du même
fichier — mettait à jour une ligne existante sans changer `recorded_at`, et les rapports filtrés sur
`recorded_at` affichaient 0 transaction depuis le 01/11. Les rapports du 31/10 au 09/11 ont été
régénérés après correction.

**Pourquoi `record_metrics` fait échouer le run.** Airflow juge l'état d'un run sur ses tâches
terminales. `store_in_db` étant en `all_done`, un run dont l'appel à l'API avait échoué finissait…
vert. `record_metrics`, nouvelle tâche terminale, enregistre les métriques puis lève une exception si
une tâche a échoué : le run est rouge, et l'échec est quand même mesuré.

**Pourquoi le rattrapage pour le rapport et pas pour le temps réel.** L'API ne sert que la
transaction courante : il n'y a rien à rattraper, `catchup=False`. Le rapport, lui, porte sur des
données déjà en base : `catchup=True` produit automatiquement les rapports des jours où la plateforme
était arrêtée.

## 8. Une seconde source : l'ingestion de fichiers

Le DAG `fraud_batch_ingest` surveille `data/inbox/` toutes les 5 minutes. Il démontre ce que l'API ne
permet pas de montrer : des **formats** variés (CSV, JSON, JSON Lines, Parquet) et des **volumes**
variables. La lecture se fait par lots de 50 000 lignes : la mémoire consommée ne dépend pas de la
taille du fichier. Mesure : 555 719 lignes contrôlées, scorées et stockées en 36,7 s.

Le déclencheur est un `ShortCircuitOperator` qui liste le dossier, plutôt qu'un `FileSensor` : il
ne dépend d'aucune connexion `fs_default` et ne monopolise pas un slot de worker à attendre.

## 9. Le monitoring

Détail : [`MONITORING.md`](MONITORING.md).

**Pourquoi stocker les métriques dans `fraud-db`** plutôt que de lire la base de métadonnées
d'Airflow : les métadonnées contiennent les états des tâches, mais pas le métier (lignes traitées,
rejets, fraudes, latence de l'API, version du modèle). Une table dédiée, écrite par chaque DAG,
réunit les deux. Le champ `extra` (JSONB) permet d'ajouter une métrique sans migration.

**Pourquoi Grafana.** Un tableau de bord versionné (`grafana/dashboards/`), lisible par quelqu'un qui
n'ouvre jamais Airflow, branché en **lecture seule** sur le seul schéma `monitoring`. Une pile
Prometheus + StatsD aurait ajouté deux conteneurs pour des métriques d'infrastructure, pas les
métriques métier dont les responsables métier et l'équipe fraude ont besoin.

**Pourquoi un DAG de surveillance séparé.** Les règles d'alerte (arrêt, échecs, durée, latence,
rejets, dérive, rapport manquant) tournent dans `pipeline_monitoring`, sans dépendance Airflow avec
les DAGs surveillés : si l'un d'eux est bloqué, le surveillant continue d'alerter.

## 10. Le RGPD

Détail : [`RGPD.md`](RGPD.md).

La minimisation était déjà acquise (filtrage 7/21 colonnes, §3.2). Le reste repose sur trois
principes :

- **tout traitement sur une donnée personnelle laisse une trace** : chaque ligne porte sa source, son
  run et sa version de modèle ; `audit.journal` consigne accès, rectifications, effacements, purges,
  sauvegardes et incidents ;
- **les obligations récurrentes sont automatisées** : purge de rétention et sauvegarde chaque nuit
  (`rgpd_maintenance`), détection d'incidents toutes les 10 minutes ;
- **les droits sont un traitement comme un autre** : un DAG à formulaire (`rgpd_requests`) plutôt
  que des requêtes SQL à la main, pour que chaque demande soit exécutée de la même façon et tracée.

## 11. L'organisation du code

Les fichiers de DAG ne décrivent que l'enchaînement des tâches ; la logique vit dans le paquet
`dags/fraud_pipeline/` (8 modules à responsabilité unique). Deux raisons : la même logique sert
plusieurs DAGs (contrôle qualité, upsert, métriques), et elle se **teste sans Airflow** — les
fonctions de contrôle et les règles d'alerte sont pures. 29 tests pytest, un lint `ruff` et une
intégration continue GitHub Actions détectent un DAG cassé au commit, pas au premier run.

