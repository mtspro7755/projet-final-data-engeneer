# Documentation : Déploiement ETL et Utilisation des Tableaux de Bord BI

Ce document fournit les instructions nécessaires pour déployer le pipeline ETL basé sur Apache Spark et pour utiliser les tableaux de bord Business Intelligence développés avec Power BI.

---

## Partie 1 : Déploiement du Pipeline ETL (Apache Spark & PostgreSQL)

Le pipeline ETL extrait les données de SQL Server (AdventureWorks), les transforme en un modèle en étoile, puis les charge dans PostgreSQL comme entrepôt de données.

### ✅ Prérequis

* Docker + Docker Compose
* Scripts :

  * ETL compilé (ex: `adventureworks-etl_2.12-1.0.jar`)
  * Pilotes JDBC (SQL Server et PostgreSQL)
  * `docker-compose.yml`
  * Script DDL PostgreSQL (création des tables)

### 📁 Structure des répertoires

```
mon-projet-final-data-engeneer/
├── captures/
├── livrable/
│   └── adventure_warehouse.sql
│   └── AdventureWorksETL.scala
│   └── BI_AdventureWorks.pbix
│   └── Documentation  Instructions pour déployer l'ETL et utiliser les tableaux de bord..txt 
├── etl/
│   ├── lib/
│   ├── mssql-jdbc-12.8.1.jre11.jar
│   └── postgresql-42.7.3.jar
│   └── postgresql-42.7.3.jar
│   ├── src/
│      └── main/
│       └── scala
│         └── AdventureWorksETL.scala
│   ├── target/scala-2.12/
│       └── adventureworksetl_2.12-1.0.jar
```

### 💡 Exemple `docker-compose.yml`

```yaml
version: '3.8'
services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      ACCEPT_EULA: Y
      SA_PASSWORD: "password123?"
    ports:
      - "1433:1433"
    volumes:
      - ./sql_server_init:/docker-entrypoint-initdb.d
    networks:
      - etl_network

  postgresql:
    image: postgres:13
    environment:
      POSTGRES_DB: adventure_warehouse
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: "pass"
    ports:
      - "5432:5432"
    networks:
      - etl_network

  spark-master:
    image: bitnami/spark:3.3.0
    command: /opt/bitnami/spark/bin/spark-shell
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      SPARK_MODE: master
    networks:
      - etl_network

  spark-worker:
    image: bitnami/spark:3.3.0
    command: /opt/bitnami/spark/bin/spark-worker --master spark://spark-master:7077
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
    networks:
      - etl_network

  etl-job:
    image: bitnami/spark:3.3.0
    command: >
      /opt/bitnami/spark/bin/spark-submit \
      --class AdventureWorksETL \
      --master spark://spark-master:7077 \
      --jars /app/lib/mssql-jdbc-12.8.1.jre11.jar,/app/lib/postgresql-42.7.3.jar \
      /app/spark_app/target/scala-2.12/adventureworks-etl_2.12-1.0.jar
    volumes:
      - ./lib:/app/lib
      - ./spark_app:/app/spark_app
    depends_on:
      - sqlserver
      - postgresql
      - spark-master
      - spark-worker
    networks:
      - etl_network

networks:
  etl_network:
    driver: bridge
```

### 🚀 Lancer le déploiement

```bash
docker-compose up --build -d
docker-compose logs etl-job
```

---

## Partie 2 : Utilisation des Tableaux de Bord Power BI

### 📂 Connexion PostgreSQL > Power BI

1. Ouvrir Power BI Desktop
2. Accueil > Obtenir les données > PostgreSQL
3. Serveur : `localhost` ou IP du conteneur, DB : `adventure_warehouse`
4. S'authentifier : `postgres / pass`
5. Sélectionner les tables : `DimCustomer`, `DimDate`, `DimGeography`, `DimProduct`, `FactCustomSales`
6. Cliquez sur **Charger**

### 🔄 Modèle de données

* Vue Modèle > Assurez-vous que `FactCustomSales` est liée aux dimensions via les clés
* Cardinalité : \* vers 1
* Direction : sens unique (de la dimension vers la table de faits)

### 📊 Création des visualisations

#### 1. ⚡ Ventes par période

* Graphique en courbes ou colonnes
* Axe : `DimDate[FullDate]`
* Valeurs : `FactCustomSales[SalesAmount]`

#### 2. 🌟 Chiffre d’affaires par catégorie

* Graphique à secteurs/barres
* Axe/légende : `DimProduct[ProductCategory]`
* Valeurs : `SalesAmount`

#### 3. 🌍 Répartition géographique

* Carte ou graphique à barres
* Axe : `CountryRegion`, `StateProvince`, `City`
* Valeurs : `SalesAmount`

#### 4. 📈 Répartition par segment client

* Graphique en anneau ou secteur
* Axe/légende : `DimCustomer[CustomerType]`
* Valeurs : `SalesAmount`

#### 5. 📊 Taux de croissance des ventes

**Mesures DAX à créer** :

```DAX
TotalVentes = SUM(FactCustomSales[SalesAmount])

Sales Last Year = CALCULATE([TotalVentes], SAMEPERIODLASTYEAR(DimDate[FullDate]))

TauxCroissance = IF(
    NOT ISBLANK([Sales Last Year]) && [Sales Last Year] <> 0,
    DIVIDE([TotalVentes] - [Sales Last Year], [Sales Last Year]),
    BLANK()
)
```

* Formater `TauxCroissance` en pourcentage
* Visuel : **Carte** (pour KPI global) ou **colonne** (par année)

### 🔖 Sauvegarde et partage

* Fichier > Enregistrer sous > `BI_AdventureWorks.pbix`
* Partage : Power BI Service (compte Pro ou Premium)

---

> ✅ Cette documentation vous permet de déployer un pipeline ETL complet et de construire des visualisations BI puissantes basées sur vos données AdventureWorks.
