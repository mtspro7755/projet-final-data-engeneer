from pyspark.sql import SparkSession

# Crée une session Spark
spark = SparkSession.builder \
    .appName("AdventureWorks ETL") \
    .config("spark.jars", "/app/lib/mssql-jdbc-12.8.1.jre11.jar") \
    .getOrCreate()


# Paramètres JDBC
#jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=AdventureWorks"
jdbc_url = "jdbc:sqlserver://sqlserver:1433;databaseName=AdventureWorks;encrypt=true;trustServerCertificate=true"
connection_properties = {
    "user": "SA",
    "password": "password123?",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# --- Extraction des tables pour DimCustomer ---
print("Extracting Sales.Customer...")
df_customer = spark.read.jdbc(
    url=jdbc_url,
    table="Sales.Customer",
    properties=connection_properties
)
print("Sales.Customer extracted. Schema:")
df_customer.printSchema()
df_customer.show(5)

print("\nExtracting Person.Person...")
df_person = spark.read.jdbc(
    url=jdbc_url,
    table="Person.Person",
    properties=connection_properties
)
print("Person.Person extracted. Schema:")
df_person.printSchema()
df_person.show(5)

# --- Extraction des tables pour DimProduct ---
print("\nExtracting Production.Product...")
df_product = spark.read.jdbc(
    url=jdbc_url,
    table="Production.Product",
    properties=connection_properties
)
print("Production.Product extracted. Schema:")
df_product.printSchema()
df_product.show(5)

print("\nExtracting Production.ProductCategory...")
df_product_category = spark.read.jdbc(
    url=jdbc_url,
    table="Production.ProductCategory",
    properties=connection_properties
)
print("Production.ProductCategory extracted. Schema:")
df_product_category.printSchema()
df_product_category.show(5)

print("\nExtracting Production.ProductSubcategory...")
df_product_subcategory = spark.read.jdbc(
    url=jdbc_url,
    table="Production.ProductSubcategory",
    properties=connection_properties
)
print("Production.ProductSubcategory extracted. Schema:")
df_product_subcategory.printSchema()
df_product_subcategory.show(5)

# --- Extraction des tables pour DimGeography ---
print("\nExtracting Person.Address...")
df_address = spark.read.jdbc(
    url=jdbc_url,
    table="Person.Address",
    properties=connection_properties
)
print("Person.Address extracted. Schema:")
df_address.printSchema()
df_address.show(5)

print("\nExtracting Person.StateProvince...")
df_state_province = spark.read.jdbc(
    url=jdbc_url,
    table="Person.StateProvince",
    properties=connection_properties
)
print("Person.StateProvince extracted. Schema:")
df_state_province.printSchema()
df_state_province.show(5)

print("\nExtracting Person.CountryRegion...")
df_country_region = spark.read.jdbc(
    url=jdbc_url,
    table="Person.CountryRegion",
    properties=connection_properties
)
print("Person.CountryRegion extracted. Schema:")
df_country_region.printSchema()
df_country_region.show(5)


# --- Extraction des tables pour FactCustomSales ---
print("\nExtracting Sales.SalesOrderHeader...")
df_sales_order_header = spark.read.jdbc(
    url=jdbc_url,
    table="Sales.SalesOrderHeader",
    properties=connection_properties
)
print("Sales.SalesOrderHeader extracted. Schema:")
df_sales_order_header.printSchema()
df_sales_order_header.show(5)

print("\nExtracting Sales.SalesOrderDetail...")
df_sales_order_detail = spark.read.jdbc(
    url=jdbc_url,
    table="Sales.SalesOrderDetail",
    properties=connection_properties
)
print("Sales.SalesOrderDetail extracted. Schema:")
df_sales_order_detail.printSchema()
df_sales_order_detail.show(5)

print("\nAll required tables extracted successfully!")

# Arrêter la session Spark à la fin du script
spark.stop()
