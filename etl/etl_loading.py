from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, to_date, year, month, dayofmonth, dayofweek, weekofyear, quarter, date_format, lpad, when, coalesce, sum
from pyspark.sql.types import IntegerType, DateType, BooleanType, DecimalType,  ShortType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# 1. Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("AdventureWorks ETL") \
    .config("spark.jars", "/app/lib/mssql-jdbc-12.8.1.jre11.jar,/app/lib/postgresql-42.7.3.jar") \
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

print("Extracting Person.EmailAddress...")
df_email_address = spark.read.jdbc(url=jdbc_url, table="Person.EmailAddress", properties=connection_properties)
print("Person.EmailAddress extracted.")


print("Extracting Sales.SalesOrderHeader...")
df_sales_order_header = spark.read.jdbc(url=jdbc_url, table="Sales.SalesOrderHeader", properties=connection_properties)
print("Sales.SalesOrderHeader extracted.")

print("Extracting Sales.SalesOrderDetail...")
df_sales_order_detail = spark.read.jdbc(url=jdbc_url, table="Sales.SalesOrderDetail", properties=connection_properties)
print("Sales.SalesOrderDetail extracted.")

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


print("\n--- 4. Transformations et Création des Dimensions et de la table de Faits ---")

# --- DimCustomer ---
print("Creating DimCustomer...")
window_spec_customer = Window.orderBy(col("CustomerID"))
# Création d'un DataFrame temporaire qui inclut CustomerID pour la jointure avec la table de faits
df_dim_customer_enriched = df_customer.alias("c").join(
    df_person.alias("p"),
    col("c.PersonID") == col("p.BusinessEntityID"),
    "left"
).join(
    df_email_address.alias("ea"),
    col("p.BusinessEntityID") == col("ea.BusinessEntityID"),
    "left"
).select(
    col("c.CustomerID"), # Garder CustomerID pour la jointure avec la table de faits
    row_number().over(window_spec_customer).cast(IntegerType()).alias("CustomerKey"),
    col("p.FirstName"),
    col("p.LastName"),
    col("ea.EmailAddress"),
    when(col("c.StoreID").isNotNull(), lit("Store")).otherwise(lit("Individual")).alias("CustomerType")
)

# DataFrame final pour DimCustomer (selon le DDL)
df_dim_customer = df_dim_customer_enriched.select(
    col("CustomerKey"),
    col("FirstName").cast(StringType()).alias("FirstName"),
    col("LastName").cast(StringType()).alias("LastName"),
    col("EmailAddress").cast(StringType()).alias("EmailAddress"),
    col("CustomerType").cast(StringType()).alias("CustomerType")
)
print("DimCustomer created.")
df_dim_customer.printSchema()
df_dim_customer.show(5)


# --- DimProduct ---
print("Creating DimProduct...")
window_spec_product = Window.orderBy(col("ProductID"))
# Création d'un DataFrame temporaire qui inclut ProductID pour la jointure avec la table de faits
df_dim_product_enriched = df_product.alias("p").join(
    df_product_subcategory.alias("psc"),
    col("p.ProductSubcategoryID") == col("psc.ProductSubcategoryID"),
    "left"
).join(
    df_product_category.alias("pc"),
    col("psc.ProductCategoryID") == col("pc.ProductCategoryID"),
    "left"
).select(
    col("p.ProductID"), # Garder ProductID pour la jointure avec la table de faits
    row_number().over(window_spec_product).cast(IntegerType()).alias("ProductKey"),
    col("p.Name").alias("ProductName"),
    col("p.ProductNumber"),
    col("pc.Name").alias("ProductCategory"),
    col("psc.Name").alias("ProductSubcategory")
)

# DataFrame final pour DimProduct (selon le DDL)
df_dim_product = df_dim_product_enriched.select(
    col("ProductKey"),
    col("ProductName").cast(StringType()).alias("ProductName"),
    col("ProductNumber").cast(StringType()).alias("ProductNumber"),
    col("ProductCategory").cast(StringType()).alias("ProductCategory"),
    col("ProductSubcategory").cast(StringType()).alias("ProductSubcategory")
)
print("DimProduct created.")
df_dim_product.printSchema()
df_dim_product.show(5)


# --- DimGeography ---
print("Creating DimGeography...")
window_spec_geography = Window.orderBy(col("AddressID"))
# Création d'un DataFrame temporaire qui inclut AddressID pour la jointure avec la table de faits
df_dim_geography_enriched = df_address.alias("a").join(
    df_state_province.alias("sp"),
    col("a.StateProvinceID") == col("sp.StateProvinceID"),
    "left"
).join(
    df_country_region.alias("cr"),
    col("sp.CountryRegionCode") == col("cr.CountryRegionCode"),
    "left"
).select(
    col("a.AddressID"), # Garder AddressID pour la jointure avec la table de faits
    row_number().over(window_spec_geography).cast(IntegerType()).alias("GeographyKey"),
    col("a.City"),
    col("sp.Name").alias("StateProvince"),
    col("cr.Name").alias("CountryRegion"),
    col("a.PostalCode")
)

# DataFrame final pour DimGeography (selon le DDL)
df_dim_geography = df_dim_geography_enriched.select(
    col("GeographyKey"),
    col("City").cast(StringType()).alias("City"),
    col("StateProvince").cast(StringType()).alias("StateProvince"),
    col("CountryRegion").cast(StringType()).alias("CountryRegion"),
    col("PostalCode").cast(StringType()).alias("PostalCode")
)
print("DimGeography created.")
df_dim_geography.printSchema()
df_dim_geography.show(5)


# --- DimDate ---
print("Creating DimDate...")
# Créer un DataFrame de dates unique à partir de OrderDate
df_dates = df_sales_order_header.select(col("OrderDate").alias("FullDate")).distinct()
window_spec_date = Window.orderBy(col("FullDate"))
# Création d'un DataFrame temporaire qui inclut FullDate pour la jointure avec la table de faits
df_dim_date_enriched = df_dates.select(
    col("FullDate"), # Garder FullDate pour la jointure avec la table de faits
    (year(col("FullDate")) * 10000 + month(col("FullDate")) * 100 + dayofmonth(col("FullDate"))).cast(IntegerType()).alias("DateKey"),
    year(col("FullDate")).cast(IntegerType()).alias("Year"),
    month(col("FullDate")).cast(IntegerType()).alias("Month"),
    date_format(col("FullDate"), "MMMM").alias("MonthName"),
    quarter(col("FullDate")).cast(IntegerType()).alias("Quarter")
)

# DataFrame final pour DimDate (selon le DDL)
df_dim_date = df_dim_date_enriched.select(
    col("DateKey"),
    col("FullDate"),
    col("Year"),
    col("Month"),
    col("MonthName").cast(StringType()).alias("MonthName"),
    col("Quarter")
)
print("DimDate created.")
df_dim_date.printSchema()
df_dim_date.show(5)


# --- FactCustomSales ---
print("Creating FactCustomSales...")
df_fact_custom_sales = df_sales_order_detail.alias("sod").join(
    df_sales_order_header.alias("soh"),
    col("sod.SalesOrderID") == col("soh.SalesOrderID"),
    "inner"
).join(
    # Jointure avec la dimension client enrichie pour obtenir CustomerKey
    df_dim_customer_enriched.alias("dc"), # Alias pour la dimension enrichie
    col("soh.CustomerID") == col("dc.CustomerID"), # Utiliser l'alias dc.CustomerID
    "inner"
).join(
    # Jointure avec la dimension produit enrichie pour obtenir ProductKey
    df_dim_product_enriched.alias("dp"), # Alias pour la dimension enrichie
    col("sod.ProductID") == col("dp.ProductID"), # Utiliser l'alias dp.ProductID
    "inner"
).join(
    # Jointure avec la dimension géographie enrichie pour obtenir GeographyKey
    df_dim_geography_enriched.alias("dg"), # Alias pour la dimension enrichie
    col("soh.BillToAddressID") == col("dg.AddressID"), # Utiliser l'alias dg.AddressID
    "inner"
).join(
    # Jointure avec la dimension date enrichie pour obtenir DateKey
    df_dim_date_enriched.alias("dd"), # Alias pour la dimension enrichie
    to_date(col("soh.OrderDate")) == col("dd.FullDate"), # Utiliser l'alias dd.FullDate
    "inner"
).select(
    col("dc.CustomerKey"), # Sélectionner les clés des dimensions enrichies avec leurs alias
    col("dp.ProductKey"),
    col("dg.GeographyKey"),
    col("dd.DateKey"),
    col("sod.OrderQty").cast(ShortType()).alias("OrderQuantity"),
    col("sod.LineTotal").cast(DecimalType(19, 4)).alias("SalesAmount"),
    col("sod.UnitPrice").cast(DecimalType(19, 4)).alias("UnitPrice")
)

# Appliquer le schéma exact du DDL pour FactCustomSales
df_fact_custom_sales = df_fact_custom_sales.select(
    col("CustomerKey"),
    col("ProductKey"),
    col("GeographyKey"),
    col("DateKey"),
    col("OrderQuantity"),
    col("SalesAmount"),
    col("UnitPrice")
)
print("FactCustomSales created.")
df_fact_custom_sales.printSchema()
df_fact_custom_sales.show(5)


# --- 5. Chargement des Données Transformées dans PostgreSQL ---
print("\n--- Starting data loading into PostgreSQL ---")

# Paramètres JDBC pour PostgreSQL
#jdbc_url_pg = "jdbc:postgresql://postgresql:5432/adventure_warehouse"
jdbc_url_pg = "jdbc:postgresql://host.docker.internal:5432/adventure_warehouse"
connection_properties_pg = {
    "user": "postgres",
    "password": "pass",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}






# Charger et écrire FactCustomSales
print("Loading FactCustomSales to PostgreSQL...")
df_fact_custom_sales.write.jdbc(url=jdbc_url_pg, table='public."FactCustomSales"', mode="overwrite", properties=connection_properties_pg)
print("FactCustomSales loaded to PostgreSQL.")

# Charger et écrire DimDate
print("Loading DimDate to PostgreSQL...")
df_dim_date.write.jdbc(url=jdbc_url_pg, table='public."DimDate"', mode="overwrite", properties=connection_properties_pg)
print("DimDate loaded to PostgreSQL.")

# Charger et écrire DimCustomer
print("Loading DimCustomer to PostgreSQL...")
df_dim_customer.write.jdbc(url=jdbc_url_pg, table='public."DimCustomer"', mode="overwrite", properties=connection_properties_pg)
print("DimCustomer loaded to PostgreSQL.")

# Charger et écrire DimProduct
print("Loading DimProduct to PostgreSQL...")
df_dim_product.write.jdbc(url=jdbc_url_pg, table='public."DimProduct"', mode="overwrite", properties=connection_properties_pg)
print("DimProduct loaded to PostgreSQL.")

# Charger et écrire DimGeography
print("Loading DimGeography to PostgreSQL...")
df_dim_geography.write.jdbc(url=jdbc_url_pg, table='public."DimGeography"', mode="overwrite", properties=connection_properties_pg)
print("DimGeography loaded to PostgreSQL.")

print("\n--- All data loaded into PostgreSQL. ---")

# Arrêter la session Spark
spark.stop()
print("Spark session stopped.")