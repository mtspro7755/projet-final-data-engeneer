-- #############################################
-- # Script DDL pour recréer les tables du Data Warehouse
-- # dans PostgreSQL (schéma public)
-- #
-- # ATTENTION: Ceci supprimera toutes les données existantes
-- # dans ces tables avant de les recréer.
-- #############################################

-- Suppression de la table de faits en premier (car elle a des clés étrangères)
DROP TABLE IF EXISTS public."FactCustomSales";

-- Suppression des tables de dimensions
DROP TABLE IF EXISTS public."DimCustomer";
DROP TABLE IF EXISTS public."DimProduct";
DROP TABLE IF EXISTS public."DimGeography";
DROP TABLE IF EXISTS public."DimDate";

-- #############################################
-- # Création des tables de Dimensions
-- #############################################

-- Dimension Customer
CREATE TABLE public."DimCustomer" (
    "CustomerKey" INT PRIMARY KEY,
    "FirstName" VARCHAR(50),
    "LastName" VARCHAR(50),
    "EmailAddress" VARCHAR(50),
    "CustomerType" VARCHAR(15)
);

-- Index sur les noms et l'email pour des recherches rapides (facultatif mais recommandé)
CREATE INDEX IF NOT EXISTS idx_dimcustomer_firstname ON public."DimCustomer" ("FirstName");
CREATE INDEX IF NOT EXISTS idx_dimcustomer_lastname ON public."DimCustomer" ("LastName");
CREATE INDEX IF NOT EXISTS idx_dimcustomer_emailaddress ON public."DimCustomer" ("EmailAddress");


-- Dimension Product
CREATE TABLE public."DimProduct" (
    "ProductKey" INT PRIMARY KEY,
    "ProductName" VARCHAR(100),
    "ProductNumber" VARCHAR(25),
    "ProductCategory" VARCHAR(50),
    "ProductSubcategory" VARCHAR(50)
);

-- Index sur les noms de produits et catégories pour des recherches rapides
CREATE INDEX IF NOT EXISTS idx_dimproduct_productname ON public."DimProduct" ("ProductName");
CREATE INDEX IF NOT EXISTS idx_dimproduct_productcategory ON public."DimProduct" ("ProductCategory");
CREATE INDEX IF NOT EXISTS idx_dimproduct_productsubcategory ON public."DimProduct" ("ProductSubcategory");


-- Dimension Geography
CREATE TABLE public."DimGeography" (
    "GeographyKey" INT PRIMARY KEY,
    "City" VARCHAR(30),
    "StateProvince" VARCHAR(50),
    "CountryRegion" VARCHAR(50),
    "PostalCode" VARCHAR(15)
);

-- Index sur la ville, l'état/province et le pays pour des recherches rapides
CREATE INDEX IF NOT EXISTS idx_dimgeography_city ON public."DimGeography" ("City");
CREATE INDEX IF NOT EXISTS idx_dimgeography_stateprovince ON public."DimGeography" ("StateProvince");
CREATE INDEX IF NOT EXISTS idx_dimgeography_countryregion ON public."DimGeography" ("CountryRegion");


-- Dimension Date
CREATE TABLE public."DimDate" (
    "DateKey" INT PRIMARY KEY,
    "FullDate" DATE,
    "Year" INT,
    "Month" INT,
    "MonthName" VARCHAR(10),
    "Quarter" INT
);

-- Index sur les colonnes de date fréquemment utilisées pour des filtres temporels
CREATE INDEX IF NOT EXISTS idx_dimdate_fulldate ON public."DimDate" ("FullDate");
CREATE INDEX IF NOT EXISTS idx_dimdate_year ON public."DimDate" ("Year");
CREATE INDEX IF NOT EXISTS idx_dimdate_month ON public."DimDate" ("Month");
CREATE INDEX IF NOT EXISTS idx_dimdate_quarter ON public."DimDate" ("Quarter");


-- #############################################
-- # Création de la table de Faits
-- #############################################

-- Table FactCustomSales
CREATE TABLE public."FactCustomSales" (
    "CustomerKey" INT NOT NULL,
    "ProductKey" INT NOT NULL,
    "GeographyKey" INT NOT NULL,
    "DateKey" INT NOT NULL,
    "OrderQuantity" SMALLINT NOT NULL,
    "SalesAmount" MONEY NOT NULL, -- PostgreSQL a un type MONEY
    "UnitPrice" MONEY NOT NULL,

    CONSTRAINT "PK_FactCustomSales" PRIMARY KEY ("CustomerKey", "ProductKey", "GeographyKey", "DateKey"),

    CONSTRAINT "FK_FactCustomSales_DimCustomer" FOREIGN KEY ("CustomerKey")
        REFERENCES public."DimCustomer" ("CustomerKey"),

    CONSTRAINT "FK_FactCustomSales_DimProduct" FOREIGN KEY ("ProductKey")
        REFERENCES public."DimProduct" ("ProductKey"),

    CONSTRAINT "FK_FactCustomSales_DimGeography" FOREIGN KEY ("GeographyKey")
        REFERENCES public."DimGeography" ("GeographyKey"),

    CONSTRAINT "FK_FactCustomSales_DimDate" FOREIGN KEY ("DateKey")
        REFERENCES public."DimDate" ("DateKey")
);

-- #############################################
-- # Création des Index pour la table de Faits
-- #############################################
-- Ces index sont essentiels pour les performances des jointures avec les dimensions
-- et les filtres sur les clés étrangères.

CREATE INDEX IF NOT EXISTS idx_fact_sales_customerkey ON public."FactCustomSales" ("CustomerKey");
CREATE INDEX IF NOT EXISTS idx_fact_sales_productkey ON public."FactCustomSales" ("ProductKey");
CREATE INDEX IF NOT EXISTS idx_fact_sales_geographykey ON public."FactCustomSales" ("GeographyKey");
CREATE INDEX IF NOT EXISTS idx_fact_sales_datekey ON public."FactCustomSales" ("DateKey");
