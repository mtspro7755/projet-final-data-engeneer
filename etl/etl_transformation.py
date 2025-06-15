from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, to_date, year, month, dayofmonth, dayofweek, weekofyear, quarter, date_format, lpad, when, coalesce
from pyspark.sql.types import IntegerType, DateType, BooleanType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# 1. Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("AdventureWorks ETL") \
    .config("spark.jars", "/app/lib/mssql-jdbc-12.8.1.jre11.jar") \
    .getOrCreate()

# 2. Paramètres JDBC
jdbc_url = "jdbc:sqlserver://sqlserver:1433;databaseName=AdventureWorks;encrypt=true;trustServerCertificate=true"
connection_properties = {
    "user": "SA",
    "password": "password123?",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("Starting data extraction from AdventureWorks...")

# --- 3. Extraction des tables sources ---
print("Extracting Sales.Customer...")
df_customer = spark.read.jdbc(url=jdbc_url, table="Sales.Customer", properties=connection_properties)
print("Sales.Customer extracted.")

print("Extracting Person.Person...")
df_person = spark.read.jdbc(url=jdbc_url, table="Person.Person", properties=connection_properties)
print("Person.Person extracted.")

# Note: PhoneNumber se trouve dans Person.PersonPhone, pas Person.Person
print("Extracting Person.PersonPhone...")
df_person_phone = spark.read.jdbc(url=jdbc_url, table="Person.PersonPhone", properties=connection_properties)
print("Person.PersonPhone extracted.")


print("Extracting Production.Product...")
df_product = spark.read.jdbc(url=jdbc_url, table="Production.Product", properties=connection_properties)
print("Production.Product extracted.")

print("Extracting Production.ProductCategory...")
df_product_category = spark.read.jdbc(url=jdbc_url, table="Production.ProductCategory", properties=connection_properties)
print("Production.ProductCategory extracted.")

print("Extracting Production.ProductSubcategory...")
df_product_subcategory = spark.read.jdbc(url=jdbc_url, table="Production.ProductSubcategory", properties=connection_properties)
print("Production.ProductSubcategory extracted.")

print("Extracting Person.Address...")
df_address = spark.read.jdbc(url=jdbc_url, table="Person.Address", properties=connection_properties)
print("Person.Address extracted.")

print("Extracting Person.StateProvince...")
df_state_province = spark.read.jdbc(url=jdbc_url, table="Person.StateProvince", properties=connection_properties)
print("Person.StateProvince extracted.")

print("Extracting Person.CountryRegion...")
df_country_region = spark.read.jdbc(url=jdbc_url, table="Person.CountryRegion", properties=connection_properties)
print("Person.CountryRegion extracted.")

print("Extracting Sales.SalesOrderHeader...")
df_sales_order_header = spark.read.jdbc(url=jdbc_url, table="Sales.SalesOrderHeader", properties=connection_properties)
# --- CORRECTION ICI : Renommer ModifiedDate avant la jointure ---
df_sales_order_header = df_sales_order_header.withColumnRenamed("ModifiedDate", "HeaderModifiedDate_Raw")
print("Sales.SalesOrderHeader extracted.")

print("Extracting Sales.SalesOrderDetail...")
df_sales_order_detail = spark.read.jdbc(url=jdbc_url, table="Sales.SalesOrderDetail", properties=connection_properties)
# --- CORRECTION ICI : Renommer ModifiedDate avant la jointure ---
df_sales_order_detail = df_sales_order_detail.withColumnRenamed("ModifiedDate", "DetailModifiedDate_Raw")
print("Sales.SalesOrderDetail extracted.")

print("\n--- All source tables extracted. Starting Transformations ---")

# --- 4. Transformation des Données ---

# --- 4.1. DimCustomer ---
print("\n--- Creating DimCustomer ---")

# Renommer les colonnes 'ModifiedDate' et 'BusinessEntityID' de df_person pour éviter les conflits
df_person_for_customer = df_person.withColumnRenamed("ModifiedDate", "Person_ModifiedDate") \
                                   .withColumnRenamed("BusinessEntityID", "Person_BusinessEntityID")

# Joindre Customer avec Person et PersonPhone
# Assurez-vous que les PersonID dans Sales.Customer correspondent à BusinessEntityID dans Person.Person
# Assurez-vous que BusinessEntityID dans Person.PersonPhone correspond à BusinessEntityID dans Person.Person
df_dim_customer_temp = df_customer.join(
    df_person_for_customer,
    df_customer["PersonID"] == df_person_for_customer["Person_BusinessEntityID"],
    "left_outer"
).join(
    df_person_phone,
    df_customer["PersonID"] == df_person_phone["BusinessEntityID"], # Joindre sur PersonID pour trouver le téléphone
    "left_outer"
)

# Création de la colonne 'Name' et sélection des colonnes
df_dim_customer = df_dim_customer_temp.select(
    col("CustomerID"),
    col("PersonID"), # Garder PersonID même si parfois nul
    col("StoreID"), # Garder StoreID même si parfois nul
    col("TerritoryID"),
    col("AccountNumber"),
    # Combiner les noms. Utilise coalesce pour gérer les MiddleName nuls
    concat_ws(" ", col("FirstName"), col("MiddleName"), col("LastName")).alias("Name"),
    # EmailPromotion est un bit (0/1), le garder comme tel ou le caster en Boolean
    col("EmailPromotion").cast(BooleanType()).alias("EmailPromotion"),
    # Il peut y avoir plusieurs numéros de téléphone par personne. Prend le premier trouvé pour cet exemple.
    col("PhoneNumber").alias("PhoneNumber"),
    # Utilise la ModifiedDate de Customer si PersonID est null, sinon la ModifiedDate de Person
    coalesce(df_customer["ModifiedDate"], col("Person_ModifiedDate")).alias("ModifiedDate")
)

# Génération de CustomerKey (clé de substitution)
window_spec_customer = Window.orderBy(col("CustomerID"))
df_dim_customer = df_dim_customer.withColumn("CustomerKey", row_number().over(window_spec_customer))

# Nettoyage : suppression des doublons sur CustomerKey (utile si CustomerID n'est pas unique dans des scénarios complexes)
df_dim_customer = df_dim_customer.dropDuplicates(["CustomerKey"])

# --- NOUVEAU NETTOYAGE : Suppression des lignes avec des valeurs nulles dans les colonnes clés/importantes de DimCustomer ---
# Nous décidons ici que CustomerKey, AccountNumber, Name et ModifiedDate sont essentiels
# PersonID et StoreID peuvent être nuls pour les clients non liés à une personne ou un magasin physique
df_dim_customer = df_dim_customer.filter(
    col("CustomerKey").isNotNull() &
    col("AccountNumber").isNotNull() &
    col("Name").isNotNull() &
    col("ModifiedDate").isNotNull()
)

# Réordonner les colonnes pour correspondre au schéma cible et s'assurer des types
df_dim_customer = df_dim_customer.select(
    col("CustomerKey").cast(IntegerType()),
    col("CustomerID").cast(IntegerType()),
    col("PersonID").cast(IntegerType()),
    col("StoreID").cast(IntegerType()),
    col("TerritoryID").cast(IntegerType()),
    col("AccountNumber"),
    col("Name"),
    col("EmailPromotion"), # Déjà casté en BooleanType
    col("PhoneNumber"),
    col("ModifiedDate").cast(DateType())
)

print("DimCustomer created. Schema:")
df_dim_customer.printSchema()
df_dim_customer.show(5, truncate=False)

# Sauvegarde de DimCustomer en Parquet
df_dim_customer.write.mode("overwrite").parquet("/app/output/dim_customer")
print("DimCustomer saved to /app/output/dim_customer")


# --- 4.2. DimProduct ---
print("\n--- Creating DimProduct ---")

# Renommer les colonnes 'Name' ET 'ModifiedDate' pour éviter les ambiguïtés après les jointures
df_product_category_renamed = df_product_category.withColumnRenamed("Name", "ProductCategoryName") \
                                                 .withColumnRenamed("ModifiedDate", "ProductCategoryModifiedDate")
df_product_subcategory_renamed = df_product_subcategory.withColumnRenamed("Name", "ProductSubcategoryName") \
                                                        .withColumnRenamed("ModifiedDate", "ProductSubcategoryModifiedDate")


# Jointure de Product, Subcategory, Category
df_dim_product_temp = df_product.join(
    df_product_subcategory_renamed,
    df_product["ProductSubcategoryID"] == df_product_subcategory_renamed["ProductSubcategoryID"],
    "left_outer"
).join(
    df_product_category_renamed,
    df_product_subcategory_renamed["ProductCategoryID"] == df_product_category_renamed["ProductCategoryID"],
    "left_outer"
)

# Sélection et renommage des colonnes
df_dim_product = df_dim_product_temp.select(
    col("ProductID"),
    col("Name").alias("Name"), # Nom du produit de df_product
    col("ProductNumber"),
    col("MakeFlag").cast(BooleanType()),
    col("FinishedGoodsFlag").cast(BooleanType()),
    col("Color"),
    col("SafetyStockLevel").cast(IntegerType()),
    col("ReorderPoint").cast(IntegerType()),
    col("StandardCost").cast(DecimalType(10,4)),
    col("ListPrice").cast(DecimalType(10,4)),
    col("Size"),
    col("SizeUnitMeasureCode"),
    col("Weight").cast(DecimalType(10,2)),
    col("WeightUnitMeasureCode"),
    col("DaysToManufacture").cast(IntegerType()),
    col("ProductLine"),
    col("Class"),
    col("Style"),
    col("ProductCategoryName"),
    col("ProductSubcategoryName"),
    col("ModifiedDate").alias("ModifiedDate").cast(DateType()) # ModifiedDate de df_product
)

# Génération de ProductKey
window_spec_product = Window.orderBy(col("ProductID"))
df_dim_product = df_dim_product.withColumn("ProductKey", row_number().over(window_spec_product))

# Nettoyage : suppression des doublons sur ProductKey
df_dim_product = df_dim_product.dropDuplicates(["ProductKey"])

# --- NOUVEAU NETTOYAGE : Suppression des lignes avec des valeurs nulles dans les colonnes clés/importantes de DimProduct ---
# ProductKey, ProductID, Name, ProductNumber, StandardCost, ListPrice, ModifiedDate sont considérés essentiels
df_dim_product = df_dim_product.filter(
    col("ProductKey").isNotNull() &
    col("ProductID").isNotNull() &
    col("Name").isNotNull() &
    col("ProductNumber").isNotNull() &
    col("StandardCost").isNotNull() &
    col("ListPrice").isNotNull() &
    col("ModifiedDate").isNotNull()
    # ProductCategoryName et ProductSubcategoryName peuvent rester null pour les produits sans catégorie/sous-catégorie
)

# Réordonner et caster les colonnes (les casts sont déjà faits ci-dessus)
df_dim_product = df_dim_product.select(
    col("ProductKey"),
    col("ProductID"),
    col("Name"),
    col("ProductNumber"),
    col("MakeFlag"),
    col("FinishedGoodsFlag"),
    col("Color"),
    col("SafetyStockLevel"),
    col("ReorderPoint"),
    col("StandardCost"),
    col("ListPrice"),
    col("Size"),
    col("SizeUnitMeasureCode"),
    col("Weight"),
    col("WeightUnitMeasureCode"),
    col("DaysToManufacture"),
    col("ProductLine"),
    col("Class"),
    col("Style"),
    col("ProductCategoryName"),
    col("ProductSubcategoryName"),
    col("ModifiedDate")
)

print("DimProduct created. Schema:")
df_dim_product.printSchema()
df_dim_product.show(5, truncate=False)

# Sauvegarde de DimProduct en Parquet
df_dim_product.write.mode("overwrite").parquet("/app/output/dim_product")
print("DimProduct saved to /app/output/dim_product")


# --- 4.3. DimGeography ---
print("\n--- Creating DimGeography ---")

# Renommer les colonnes 'Name' et 'ModifiedDate' pour éviter les ambiguïtés
df_state_province_renamed = df_state_province.withColumnRenamed("Name", "StateProvinceName") \
                                             .withColumnRenamed("ModifiedDate", "StateModifiedDate")
df_country_region_renamed = df_country_region.withColumnRenamed("Name", "CountryRegionName") \
                                             .withColumnRenamed("ModifiedDate", "CountryModifiedDate") \
                                             .withColumnRenamed("CountryRegionCode", "CountryRegionCode_From_Country")

# Jointure de Address, StateProvince, CountryRegion
df_dim_geography_temp = df_address.join(
    df_state_province_renamed,
    df_address["StateProvinceID"] == df_state_province_renamed["StateProvinceID"],
    "left_outer"
).join(
    df_country_region_renamed,
    df_state_province_renamed["CountryRegionCode"] == df_country_region_renamed["CountryRegionCode_From_Country"],
    "left_outer"
)

# Sélection et renommage des colonnes
df_dim_geography = df_dim_geography_temp.select(
    col("AddressID"),
    col("AddressLine1"),
    col("AddressLine2"),
    col("City"),
    col("StateProvinceCode"),
    col("StateProvinceName"),
    col("CountryRegionCode"),
    col("CountryRegionName"),
    col("PostalCode"),
    col("ModifiedDate").alias("ModifiedDate").cast(DateType())
)

# Génération de GeographyKey
window_spec_geography = Window.orderBy(col("AddressID"))
df_dim_geography = df_dim_geography.withColumn("GeographyKey", row_number().over(window_spec_geography))

# Nettoyage : suppression des doublons sur GeographyKey
df_dim_geography = df_dim_geography.dropDuplicates(["GeographyKey"])

# --- NOUVEAU NETTOYAGE : Suppression des lignes avec des valeurs nulles dans les colonnes clés/importantes de DimGeography ---
# GeographyKey, AddressID, AddressLine1, City, PostalCode, ModifiedDate sont considérés essentiels
# StateProvinceName, CountryRegionName, AddressLine2 peuvent être nuls
df_dim_geography = df_dim_geography.filter(
    col("GeographyKey").isNotNull() &
    col("AddressID").isNotNull() &
    col("AddressLine1").isNotNull() &
    col("City").isNotNull() &
    col("PostalCode").isNotNull() &
    col("ModifiedDate").isNotNull()
)

# Réordonner et caster les colonnes
df_dim_geography = df_dim_geography.select(
    col("GeographyKey").cast(IntegerType()),
    col("AddressID").cast(IntegerType()),
    col("AddressLine1"),
    col("AddressLine2"),
    col("City"),
    col("StateProvinceCode"),
    col("StateProvinceName"),
    col("CountryRegionCode"),
    col("CountryRegionName"),
    col("PostalCode"),
    col("ModifiedDate")
)

print("DimGeography created. Schema:")
df_dim_geography.printSchema()
df_dim_geography.show(5, truncate=False)

# Sauvegarde de DimGeography en Parquet
df_dim_geography.write.mode("overwrite").parquet("/app/output/dim_geography")
print("DimGeography saved to /app/output/dim_geography")


# --- 4.4. DimDate ---
print("\n--- Creating DimDate ---")

# Extraire toutes les dates uniques des tables de faits (OrderDate, DueDate, ShipDate)
# Convertir en DateType pour s'assurer de l'homogénéité
df_dates_raw = df_sales_order_header.select(
    to_date(col("OrderDate")).alias("FullDateAlternateKey")
).union(
    df_sales_order_header.select(to_date(col("DueDate")).alias("FullDateAlternateKey"))
).union(
    df_sales_order_header.select(to_date(col("ShipDate")).alias("FullDateAlternateKey"))
).union(
    df_sales_order_detail.select(to_date(col("DetailModifiedDate_Raw")).alias("FullDateAlternateKey")) # Utilise le nom renommé
).distinct()

# Filtrer les dates nulles si certaines colonnes n'ont pas toujours de dates
# C'est déjà présent et important pour la dimension date.
df_dates_raw = df_dates_raw.filter(col("FullDateAlternateKey").isNotNull())

# Trier les dates pour une génération de clé cohérente
df_dates_raw = df_dates_raw.orderBy(col("FullDateAlternateKey"))

# Ajouter les colonnes de date
df_dim_date = df_dates_raw.select(
    col("FullDateAlternateKey"),
    dayofmonth(col("FullDateAlternateKey")).alias("DayNumberOfMonth"),
    date_format(col("FullDateAlternateKey"), "EEEE").alias("DayNameOfWeek"), # Nom complet du jour de la semaine
    dayofweek(col("FullDateAlternateKey")).alias("DayNumberOfWeek"), # 1 = Dimanche, 7 = Samedi
    date_format(col("FullDateAlternateKey"), "D").alias("DayNumberOfYear"), # Jour de l'année
    weekofyear(col("FullDateAlternateKey")).alias("WeekNumberOfYear"),
    month(col("FullDateAlternateKey")).alias("MonthNumberOfYear"),
    date_format(col("FullDateAlternateKey"), "MMMM").alias("MonthName"), # Nom complet du mois
    quarter(col("FullDateAlternateKey")).alias("QuarterNumberOfYear"),
    concat_ws(" ", lit("Q"), quarter(col("FullDateAlternateKey"))).alias("QuarterName"), # Ex: Q1, Q2
    year(col("FullDateAlternateKey")).alias("CalendarYear"),
    # Pour CalendarSemester, on peut dériver de QuarterNumberOfYear
    # Semestre 1 pour Q1 et Q2, Semestre 2 pour Q3 et Q4
    when(quarter(col("FullDateAlternateKey")).isin(1, 2), lit("H1"))
        .otherwise(lit("H2")).alias("CalendarSemester"),
    # FiscalQuarter et FiscalYear dépendent de la définition de l'année fiscale de l'entreprise.
    # Pour l'exemple, nous allons les aligner sur l'année civile pour la simplicité.
    # En pratique, vous auriez besoin de règles spécifiques (ex: année fiscale commence en juillet)
    col("QuarterNumberOfYear").alias("FiscalQuarter"),
    col("CalendarYear").alias("FiscalYear"),
    col("CalendarSemester").alias("FiscalSemester"),
    col("FullDateAlternateKey").alias("ModifiedDate").cast(DateType()) # Utiliser la date elle-même comme ModifiedDate
)

# Génération de DateKey (format YYYYMMDD)
df_dim_date = df_dim_date.withColumn(
    "DateKey",
    (year(col("FullDateAlternateKey")) * 10000 +
     month(col("FullDateAlternateKey")) * 100 +
     dayofmonth(col("FullDateAlternateKey"))
    ).cast(IntegerType())
)

# Nettoyage : suppression des doublons sur DateKey
df_dim_date = df_dim_date.dropDuplicates(["DateKey"])

# --- NOUVEAU NETTOYAGE : Suppression des lignes avec des valeurs nulles dans les colonnes clés/importantes de DimDate ---
# Toutes les colonnes générées devraient être non nulles si FullDateAlternateKey est non nulle,
# mais une vérification supplémentaire ne fait pas de mal pour la robustesse.
df_dim_date = df_dim_date.filter(
    col("DateKey").isNotNull() &
    col("FullDateAlternateKey").isNotNull() &
    col("CalendarYear").isNotNull()
)

# Réordonner les colonnes pour correspondre au schéma cible
df_dim_date = df_dim_date.select(
    col("DateKey").cast(IntegerType()),
    col("FullDateAlternateKey").cast(DateType()),
    col("DayNumberOfMonth").cast(IntegerType()),
    col("DayNameOfWeek"),
    col("DayNumberOfWeek").cast(IntegerType()),
    col("DayNumberOfYear").cast(IntegerType()),
    col("WeekNumberOfYear").cast(IntegerType()),
    col("MonthNumberOfYear").cast(IntegerType()),
    col("MonthName"),
    col("QuarterNumberOfYear").cast(IntegerType()),
    col("QuarterName"),
    col("CalendarYear").cast(IntegerType()),
    col("CalendarSemester"),
    col("FiscalQuarter").cast(IntegerType()),
    col("FiscalYear").cast(IntegerType()),
    col("FiscalSemester"),
    col("ModifiedDate").cast(DateType())
)

print("DimDate created. Schema:")
df_dim_date.printSchema()
df_dim_date.show(5, truncate=False)

# Sauvegarde de DimDate en Parquet
df_dim_date.write.mode("overwrite").parquet("/app/output/dim_date")
print("DimDate saved to /app/output/dim_date")


# --- 4.5. FactCustomSales ---
print("\n--- Creating FactCustomSales ---")

# 1. Jointure de SalesOrderHeader et SalesOrderDetail
df_fact_sales_temp = df_sales_order_header.join(
    df_sales_order_detail,
    df_sales_order_header["SalesOrderID"] == df_sales_order_detail["SalesOrderID"],
    "inner" # Jointure interne car les détails de commande doivent avoir un en-tête
)

# 2. Jointure avec DimCustomer pour CustomerKey
# Jointure sur CustomerID. On suppose que CustomerID dans SalesOrderHeader correspond à CustomerID dans DimCustomer
df_fact_sales_temp = df_fact_sales_temp.join(
    df_dim_customer.select("CustomerKey", "CustomerID"),
    df_fact_sales_temp["CustomerID"] == df_dim_customer["CustomerID"],
    "inner" # S'assurer que chaque vente a un client valide dans la dimension
)

# 3. Jointure avec DimProduct pour ProductKey
# Jointure sur ProductID. On suppose que ProductID dans SalesOrderDetail correspond à ProductID dans DimProduct
df_fact_sales_temp = df_fact_sales_temp.join(
    df_dim_product.select("ProductKey", "ProductID"),
    df_fact_sales_temp["ProductID"] == df_dim_product["ProductID"],
    "inner" # S'assurer que chaque vente a un produit valide dans la dimension
)

# 4. Jointure avec DimGeography pour GeographyKey (via ShipToAddressID)
# L'adresse de livraison (ShipToAddressID) ou de facturation (BillToAddressID) de SalesOrderHeader.
# Utilisons ShipToAddressID comme clé géographique principale pour les ventes.
df_fact_sales_temp = df_fact_sales_temp.join(
    df_dim_geography.select("GeographyKey", "AddressID"),
    df_fact_sales_temp["ShipToAddressID"] == df_dim_geography["AddressID"],
    "inner" # CHANGEMENT ICI : Jointure interne pour s'assurer que chaque vente a une géographie valide
)

# 5. Jointure avec DimDate pour DateKey (OrderDate)
# Crée OrderDateKey dans la table de faits pour la jointure
df_fact_sales_temp = df_fact_sales_temp.withColumn("OrderDateKey",
    (year(col("OrderDate")) * 10000 + month(col("OrderDate")) * 100 + dayofmonth(col("OrderDate"))).cast(IntegerType())
)
df_fact_sales_temp = df_fact_sales_temp.join(
    df_dim_date.select("DateKey", col("FullDateAlternateKey").alias("DimDateFullDateAlternateKey")),
    df_fact_sales_temp["OrderDateKey"] == df_dim_date["DateKey"],
    "inner" # S'assurer que chaque vente a une date valide dans la dimension
)

# Sélection et renommage des colonnes pour FactCustomSales
df_fact_custom_sales = df_fact_sales_temp.select(
    col("CustomerKey").cast(IntegerType()),
    col("ProductKey").cast(IntegerType()),
    col("GeographyKey").cast(IntegerType()),
    col("DateKey").alias("DateKey").cast(IntegerType()), # DateKey est la clé de la dimension date, pour l'OrderDate
    df_sales_order_header["SalesOrderID"].alias("SalesOrderID").cast(IntegerType()), # Spécifiez de df_sales_order_header
    df_sales_order_detail["SalesOrderDetailID"].alias("SalesOrderDetailID").cast(IntegerType()), # Spécifiez de df_sales_order_detail
    col("OrderQty").cast(IntegerType()),
    col("UnitPrice").cast(DecimalType(10,4)),
    col("UnitPriceDiscount").cast(DecimalType(10,4)),
    col("LineTotal").cast(DecimalType(10,4)), # Calculé automatiquement dans AdventureWorks
    col("OrderDate").cast(DateType()),
    col("DueDate").cast(DateType()),
    col("ShipDate").cast(DateType()),
    col("OnlineOrderFlag").cast(BooleanType()), # Caster en Boolean
    col("SalesOrderNumber"),
    col("CarrierTrackingNumber"),
    col("HeaderModifiedDate_Raw").alias("ModifiedDate").cast(DateType()) # CORRECTION : Utilise le nouveau nom HeaderModifiedDate_Raw
)

# Nettoyage : suppression des doublons sur les clés composites de la table de faits
# (SalesOrderID, SalesOrderDetailID) devraient être uniques pour chaque ligne de fait
df_fact_custom_sales = df_fact_custom_sales.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"])

# --- NOUVEAU NETTOYAGE : Suppression des lignes avec des valeurs nulles dans les colonnes clés/mesures de FactCustomSales ---
# Toutes les clés dimensionnelles (CustomerKey, ProductKey, GeographyKey, DateKey) devraient être non nulles grâce aux jointures INNER.
# Les mesures comme OrderQty, UnitPrice, LineTotal devraient également être non nulles pour une vente valide.
df_fact_custom_sales = df_fact_custom_sales.filter(
    col("CustomerKey").isNotNull() &
    col("ProductKey").isNotNull() &
    col("GeographyKey").isNotNull() &
    col("DateKey").isNotNull() &
    col("SalesOrderID").isNotNull() &
    col("SalesOrderDetailID").isNotNull() &
    col("OrderQty").isNotNull() &
    col("UnitPrice").isNotNull() &
    col("LineTotal").isNotNull() &
    col("OrderDate").isNotNull()
)

print("FactCustomSales created. Schema:")
df_fact_custom_sales.printSchema()
df_fact_custom_sales.show(5, truncate=False)

# Sauvegarde de FactCustomSales en Parquet
df_fact_custom_sales.write.mode("overwrite").parquet("/app/output/fact_custom_sales")
print("FactCustomSales saved to /app/output/fact_custom_sales")

print("\n--- All dimensions and fact table transformations completed and saved to Parquet! ---")



##################################################################################################################################


from pyspark.sql.functions import sum, countDistinct

print("\n--- Performing Aggregations ---")

# 1. Chiffre d'affaires par catégorie de produit
print("\nTotal Revenue by Product Category:")
df_revenue_by_category = df_fact_custom_sales.join(
    df_dim_product.select("ProductKey", "ProductCategoryName"),
    "ProductKey",
    "inner"
).groupBy("ProductCategoryName").agg(
    sum("LineTotal").alias("TotalRevenue")
).orderBy("TotalRevenue", ascending=False)

df_revenue_by_category.show(truncate=False)

# 2. Chiffre d'affaires par ville
print("\nTotal Revenue by City:")
df_revenue_by_city = df_fact_custom_sales.join(
    df_dim_geography.select("GeographyKey", "City"),
    "GeographyKey",
    "inner"
).groupBy("City").agg(
    sum("LineTotal").alias("TotalRevenue")
).orderBy("TotalRevenue", ascending=False)

df_revenue_by_city.show(truncate=False)

# 3. Chiffre d'affaires par année
print("\nTotal Revenue by Year:")
df_revenue_by_year = df_fact_custom_sales.join(
    df_dim_date.select("DateKey", "CalendarYear"),
    df_fact_custom_sales["DateKey"] == df_dim_date["DateKey"],
    "inner"
).groupBy("CalendarYear").agg(
    sum("LineTotal").alias("TotalRevenue")
).orderBy("CalendarYear")

df_revenue_by_year.show(truncate=False)
df_revenue_by_category.write.mode("overwrite").csv("/app/output/aggregations/revenue_by_category", header=True)
df_revenue_by_city.write.mode("overwrite").csv("/app/output/aggregations/revenue_by_city", header=True)
df_revenue_by_year.write.mode("overwrite").csv("/app/output/aggregations/revenue_by_year", header=True)


# Arrêter la session Spark à la fin du script
spark.stop()