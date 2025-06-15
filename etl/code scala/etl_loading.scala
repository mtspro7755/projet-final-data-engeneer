import org.apache.spark.sql.{SparkSession, Window, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AdventureWorksETLCombined {
  def main(args: Array[String]): Unit = {
    // 1. Initialisation de la session Spark
    val spark = SparkSession.builder
      .appName("AdventureWorks ETL Combined")
      // Assurez-vous d'avoir les JARs pour SQL Server et PostgreSQL
      .config("spark.jars", "/app/lib/mssql-jdbc-12.8.1.jre11.jar,/app/lib/postgresql-42.7.3.jar")
      .getOrCreate()

    // 2. Paramètres JDBC pour SQL Server (Source)
    val jdbcUrlSqlServer = "jdbc:sqlserver://sqlserver:1433;databaseName=AdventureWorks;encrypt=true;trustServerCertificate=true"
    val connectionPropertiesSqlServer = new java.util.Properties()
    connectionPropertiesSqlServer.put("user", "SA")
    connectionPropertiesSqlServer.put("password", "password123?")
    connectionPropertiesSqlServer.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    // 3. Paramètres JDBC pour PostgreSQL (Cible)
    // Utilisez 'postgresql:5432' si vous exécutez dans le même réseau Docker Compose
    // Utilisez 'host.docker.internal:5432' si vous exécutez Spark localement et PostgreSQL est dans Docker
    val jdbcUrlPg = "jdbc:postgresql://postgresql:5432/adventure_warehouse"
    // val jdbcUrlPg = "jdbc:postgresql://host.docker.internal:5432/adventure_warehouse" // Alternative pour local
    val connectionPropertiesPg = new java.util.Properties()
    connectionPropertiesPg.put("user", "postgres")
    connectionPropertiesPg.put("password", "pass")
    connectionPropertiesPg.put("driver", "org.postgresql.Driver")

    println("Starting full ETL pipeline from AdventureWorks to PostgreSQL...")

    // --- PHASE 1: Extraction des tables sources depuis SQL Server ---
    println("\n--- Phase 1: Extraction des données sources ---")

    println("Extracting Sales.Customer...")
    val dfCustomer = spark.read.jdbc(jdbcUrlSqlServer, "Sales.Customer", connectionPropertiesSqlServer)

    println("Extracting Person.Person...")
    val dfPerson = spark.read.jdbc(jdbcUrlSqlServer, "Person.Person", connectionPropertiesSqlServer)

    println("Extracting Person.PersonPhone...")
    val dfPersonPhone = spark.read.jdbc(jdbcUrlSqlServer, "Person.PersonPhone", connectionPropertiesSqlServer)

    println("Extracting Production.Product...")
    val dfProduct = spark.read.jdbc(jdbcUrlSqlServer, "Production.Product", connectionPropertiesSqlServer)

    println("Extracting Production.ProductCategory...")
    val dfProductCategory = spark.read.jdbc(jdbcUrlSqlServer, "Production.ProductCategory", connectionPropertiesSqlServer)

    println("Extracting Production.ProductSubcategory...")
    val dfProductSubcategory = spark.read.jdbc(jdbcUrlSqlServer, "Production.ProductSubcategory", connectionPropertiesSqlServer)

    println("Extracting Person.Address...")
    val dfAddress = spark.read.jdbc(jdbcUrlSqlServer, "Person.Address", connectionPropertiesSqlServer)

    println("Extracting Person.StateProvince...")
    val dfStateProvince = spark.read.jdbc(jdbcUrlSqlServer, "Person.StateProvince", connectionPropertiesSqlServer)

    println("Extracting Person.CountryRegion...")
    val dfCountryRegion = spark.read.jdbc(jdbcUrlSqlServer, "Person.CountryRegion", connectionPropertiesSqlServer)

    println("Extracting Sales.SalesOrderHeader...")
    val dfSalesOrderHeader = spark.read.jdbc(jdbcUrlSqlServer, "Sales.SalesOrderHeader", connectionPropertiesSqlServer)
      .withColumnRenamed("ModifiedDate", "HeaderModifiedDate_Raw")

    println("Extracting Sales.SalesOrderDetail...")
    val dfSalesOrderDetail = spark.read.jdbc(jdbcUrlSqlServer, "Sales.SalesOrderDetail", connectionPropertiesSqlServer)
      .withColumnRenamed("ModifiedDate", "DetailModifiedDate_Raw")

    println("\n--- All source tables extracted. ---")


    // --- PHASE 2: Transformation des Données ---
    println("\n--- Phase 2: Transformations des données ---")

    // --- 2.1. DimCustomer ---
    println("Creating DimCustomer...")
    val dfPersonForCustomer = dfPerson.withColumnRenamed("ModifiedDate", "Person_ModifiedDate")
                                       .withColumnRenamed("BusinessEntityID", "Person_BusinessEntityID")

    val dfDimCustomerTemp = dfCustomer.join(
      dfPersonForCustomer,
      dfCustomer("PersonID") === dfPersonForCustomer("Person_BusinessEntityID"),
      "left_outer"
    ).join(
      dfPersonPhone,
      dfCustomer("PersonID") === dfPersonPhone("BusinessEntityID"),
      "left_outer"
    )

    val windowSpecCustomer = Window.orderBy(col("CustomerID"))

    val dfDimCustomer = dfDimCustomerTemp.select(
      col("CustomerID"),
      col("PersonID"),
      col("StoreID"),
      col("TerritoryID"),
      col("AccountNumber"),
      concat_ws(" ", col("FirstName"), coalesce(col("MiddleName"), lit("")), col("LastName")).alias("Name"),
      col("EmailPromotion").cast(BooleanType).alias("EmailPromotion"),
      col("PhoneNumber").alias("PhoneNumber"),
      coalesce(dfCustomer("ModifiedDate"), col("Person_ModifiedDate")).alias("ModifiedDate")
    ).withColumn("CustomerKey", row_number().over(windowSpecCustomer))
     .dropDuplicates("CustomerKey")
     .select(
      col("CustomerKey").cast(IntegerType),
      col("CustomerID").cast(IntegerType),
      col("PersonID").cast(IntegerType),
      col("StoreID").cast(IntegerType),
      col("TerritoryID").cast(IntegerType),
      col("AccountNumber"),
      col("Name"),
      col("EmailPromotion"),
      col("PhoneNumber"),
      col("ModifiedDate").cast(DateType)
    )
    println("DimCustomer created.")


    // --- 2.2. DimProduct ---
    println("Creating DimProduct...")
    val dfProductCategoryRenamed = dfProductCategory.withColumnRenamed("Name", "ProductCategoryName")
                                                     .withColumnRenamed("ModifiedDate", "ProductCategoryModifiedDate")
    val dfProductSubcategoryRenamed = dfProductSubcategory.withColumnRenamed("Name", "ProductSubcategoryName")
                                                           .withColumnRenamed("ModifiedDate", "ProductSubcategoryModifiedDate")

    val dfDimProductTemp = dfProduct.join(
      dfProductSubcategoryRenamed,
      dfProduct("ProductSubcategoryID") === dfProductSubcategoryRenamed("ProductSubcategoryID"),
      "left_outer"
    ).join(
      dfProductCategoryRenamed,
      dfProductSubcategoryRenamed("ProductCategoryID") === dfProductCategoryRenamed("ProductCategoryID"),
      "left_outer"
    )

    val windowSpecProduct = Window.orderBy(col("ProductID"))

    val dfDimProduct = dfDimProductTemp.select(
      col("ProductID"),
      col("Name").alias("Name"),
      col("ProductNumber"),
      col("MakeFlag").cast(BooleanType),
      col("FinishedGoodsFlag").cast(BooleanType),
      col("Color"),
      col("SafetyStockLevel").cast(IntegerType),
      col("ReorderPoint").cast(IntegerType),
      col("StandardCost").cast(DecimalType(10,4)),
      col("ListPrice").cast(DecimalType(10,4)),
      col("Size"),
      col("SizeUnitMeasureCode"),
      col("Weight").cast(DecimalType(10,2)),
      col("WeightUnitMeasureCode"),
      col("DaysToManufacture").cast(IntegerType),
      col("ProductLine"),
      col("Class"),
      col("Style"),
      col("ProductCategoryName"),
      col("ProductSubcategoryName"),
      col("ModifiedDate").alias("ModifiedDate").cast(DateType)
    ).withColumn("ProductKey", row_number().over(windowSpecProduct))
     .dropDuplicates("ProductKey")
     .select(
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
    println("DimProduct created.")


    // --- 2.3. DimGeography ---
    println("Creating DimGeography...")
    val dfStateProvinceRenamed = dfStateProvince.withColumnRenamed("Name", "StateProvinceName")
                                                 .withColumnRenamed("ModifiedDate", "StateModifiedDate")
    val dfCountryRegionRenamed = dfCountryRegion.withColumnRenamed("Name", "CountryRegionName")
                                                 .withColumnRenamed("ModifiedDate", "CountryModifiedDate")
                                                 .withColumnRenamed("CountryRegionCode", "CountryRegionCode_From_Country")

    val dfDimGeographyTemp = dfAddress.join(
      dfStateProvinceRenamed,
      dfAddress("StateProvinceID") === dfStateProvinceRenamed("StateProvinceID"),
      "left_outer"
    ).join(
      dfCountryRegionRenamed,
      dfStateProvinceRenamed("CountryRegionCode") === dfCountryRegionRenamed("CountryRegionCode_From_Country"),
      "left_outer"
    )

    val windowSpecGeography = Window.orderBy(col("AddressID"))

    val dfDimGeography = dfDimGeographyTemp.select(
      col("AddressID"),
      col("AddressLine1"),
      col("AddressLine2"),
      col("City"),
      col("StateProvinceCode"),
      col("StateProvinceName"),
      col("CountryRegionCode"),
      col("CountryRegionName"),
      col("PostalCode"),
      col("ModifiedDate").alias("ModifiedDate").cast(DateType)
    ).withColumn("GeographyKey", row_number().over(windowSpecGeography))
     .dropDuplicates("GeographyKey")
     .select(
      col("GeographyKey").cast(IntegerType),
      col("AddressID").cast(IntegerType),
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
    println("DimGeography created.")


    // --- 2.4. DimDate ---
    println("Creating DimDate...")
    val dfDatesRaw = dfSalesOrderHeader.select(to_date(col("OrderDate")).alias("FullDateAlternateKey"))
      .union(dfSalesOrderHeader.select(to_date(col("DueDate")).alias("FullDateAlternateKey")))
      .union(dfSalesOrderHeader.select(to_date(col("ShipDate")).alias("FullDateAlternateKey")))
      .union(dfSalesOrderDetail.select(to_date(col("DetailModifiedDate_Raw")).alias("FullDateAlternateKey")))
      .distinct()
      .filter(col("FullDateAlternateKey").isNotNull)
      .orderBy(col("FullDateAlternateKey"))

    val dfDimDate = dfDatesRaw.select(
      col("FullDateAlternateKey"),
      dayofmonth(col("FullDateAlternateKey")).alias("DayNumberOfMonth"),
      date_format(col("FullDateAlternateKey"), "EEEE").alias("DayNameOfWeek"),
      dayofweek(col("FullDateAlternateKey")).alias("DayNumberOfWeek"),
      date_format(col("FullDateAlternateKey"), "D").alias("DayNumberOfYear"),
      weekofyear(col("FullDateAlternateKey")).alias("WeekNumberOfYear"),
      month(col("FullDateAlternateKey")).alias("MonthNumberOfYear"),
      date_format(col("FullDateAlternateKey"), "MMMM").alias("MonthName"),
      quarter(col("FullDateAlternateKey")).alias("QuarterNumberOfYear"),
      concat_ws(" ", lit("Q"), quarter(col("FullDateAlternateKey"))).alias("QuarterName"),
      year(col("FullDateAlternateKey")).alias("CalendarYear"),
      when(quarter(col("FullDateAlternateKey")).isin(1, 2), lit("H1"))
        .otherwise(lit("H2")).alias("CalendarSemester"),
      col("QuarterNumberOfYear").alias("FiscalQuarter"),
      col("CalendarYear").alias("FiscalYear"),
      col("CalendarSemester").alias("FiscalSemester"),
      col("FullDateAlternateKey").alias("ModifiedDate").cast(DateType)
    ).withColumn(
      "DateKey",
      (year(col("FullDateAlternateKey")) * 10000 +
       month(col("FullDateAlternateKey")) * 100 +
       dayofmonth(col("FullDateAlternateKey"))
      ).cast(IntegerType)
    ).dropDuplicates("DateKey")
     .select(
      col("DateKey").cast(IntegerType),
      col("FullDateAlternateKey").cast(DateType),
      col("DayNumberOfMonth").cast(IntegerType),
      col("DayNameOfWeek"),
      col("DayNumberOfWeek").cast(IntegerType),
      col("DayNumberOfYear").cast(IntegerType),
      col("WeekNumberOfYear").cast(IntegerType),
      col("MonthNumberOfYear").cast(IntegerType),
      col("MonthName"),
      col("QuarterNumberOfYear").cast(IntegerType),
      col("QuarterName"),
      col("CalendarYear").cast(IntegerType),
      col("CalendarSemester"),
      col("FiscalQuarter").cast(IntegerType),
      col("FiscalYear").cast(IntegerType),
      col("FiscalSemester"),
      col("ModifiedDate").cast(DateType)
    )
    println("DimDate created.")


    // --- 2.5. FactCustomSales ---
    println("Creating FactCustomSales...")

    val dfFactSalesTemp1 = dfSalesOrderHeader.join(
      dfSalesOrderDetail,
      dfSalesOrderHeader("SalesOrderID") === dfSalesOrderDetail("SalesOrderID"),
      "inner"
    )

    val dfFactSalesTemp2 = dfFactSalesTemp1.join(
      dfDimCustomer.select("CustomerKey", "CustomerID"), // Select only necessary columns
      dfFactSalesTemp1("CustomerID") === dfDimCustomer("CustomerID"),
      "inner"
    )

    val dfFactSalesTemp3 = dfFactSalesTemp2.join(
      dfDimProduct.select("ProductKey", "ProductID"), // Select only necessary columns
      dfFactSalesTemp2("ProductID") === dfDimProduct("ProductID"),
      "inner"
    )

    val dfFactSalesTemp4 = dfFactSalesTemp3.join(
      dfDimGeography.select("GeographyKey", "AddressID"), // Select only necessary columns
      dfFactSalesTemp3("ShipToAddressID") === dfDimGeography("AddressID"),
      "left_outer"
    )

    val dfFactSalesTemp5 = dfFactSalesTemp4.withColumn("OrderDateKey",
      (year(col("OrderDate")) * 10000 + month(col("OrderDate")) * 100 + dayofmonth(col("OrderDate"))).cast(IntegerType)
    ).join(
      dfDimDate.select("DateKey"), // Select only necessary columns
      col("OrderDateKey") === dfDimDate("DateKey"),
      "inner"
    )

    val dfFactCustomSales = dfFactSalesTemp5.select(
      col("CustomerKey").cast(IntegerType),
      col("ProductKey").cast(IntegerType),
      col("GeographyKey").cast(IntegerType),
      col("DateKey").alias("DateKey").cast(IntegerType),
      dfSalesOrderHeader("SalesOrderID").alias("SalesOrderID").cast(IntegerType),
      dfSalesOrderDetail("SalesOrderDetailID").alias("SalesOrderDetailID").cast(IntegerType),
      col("OrderQty").cast(IntegerType),
      col("UnitPrice").cast(DecimalType(10,4)),
      col("UnitPriceDiscount").cast(DecimalType(10,4)),
      col("LineTotal").cast(DecimalType(10,4)),
      col("OrderDate").cast(DateType),
      col("DueDate").cast(DateType),
      col("ShipDate").cast(DateType),
      col("OnlineOrderFlag").cast(BooleanType),
      col("SalesOrderNumber"),
      col("CarrierTrackingNumber"),
      col("HeaderModifiedDate_Raw").alias("ModifiedDate").cast(DateType)
    ).dropDuplicates("SalesOrderID", "SalesOrderDetailID")
    println("FactCustomSales created.")

    println("\n--- All dimensions and fact table transformations completed. ---")


    // --- PHASE 3: Chargement des données transformées dans PostgreSQL ---
    println("\n--- Phase 3: Chargement des données dans PostgreSQL Data Warehouse ---")

    // --- Écriture des dimensions ---
    println("Writing DimDate to PostgreSQL...")
    dfDimDate.write.mode(SaveMode.Append).jdbc(jdbcUrlPg, "public.DimDate", connectionPropertiesPg)
    println("DimDate loaded to PostgreSQL.")

    println("Writing DimCustomer to PostgreSQL...")
    dfDimCustomer.write.mode(SaveMode.Append).jdbc(jdbcUrlPg, "public.DimCustomer", connectionPropertiesPg)
    println("DimCustomer loaded to PostgreSQL.")

    println("Writing DimProduct to PostgreSQL...")
    dfDimProduct.write.mode(SaveMode.Append).jdbc(jdbcUrlPg, "public.DimProduct", connectionPropertiesPg)
    println("DimProduct loaded to PostgreSQL.")

    println("Writing DimGeography to PostgreSQL...")
    dfDimGeography.write.mode(SaveMode.Append).jdbc(jdbcUrlPg, "public.DimGeography", connectionPropertiesPg)
    println("DimGeography loaded to PostgreSQL.")

    // --- Écriture de la table de faits ---
    println("Writing FactCustomSales to PostgreSQL...")
    // Attention: Pour les tables de faits volumineuses, le mode "overwrite" peut être lent car il supprime et recrée la table.
    // Pour des mises à jour incrémentales, il faudrait une logique plus complexe (upsert, partitionnement).
    dfFactCustomSales.write.mode(SaveMode.Append).jdbc(jdbcUrlPg, "public.FactCustomSales", connectionPropertiesPg)
    println("FactCustomSales loaded to PostgreSQL.")

    println("\n--- All data loaded into PostgreSQL Data Warehouse! ---")


    // --- PHASE 4: Agrégations et écriture en CSV (Facultatif, pour rapport) ---
    // Ces agrégations sont exécutées après le chargement pour des raisons de clarté
    // mais pourraient être faites avant si les rapports sont les seuls consommateurs.
    println("\n--- Phase 4: Performing Aggregations and saving to CSV ---")

    // 1. Chiffre d'affaires par catégorie de produit
    println("Calculating Total Revenue by Product Category...")
    val dfRevenueByCategory = dfFactCustomSales.join(
      dfDimProduct.select("ProductKey", "ProductCategoryName"),
      Seq("ProductKey"),
      "inner"
    ).groupBy("ProductCategoryName").agg(
      sum("LineTotal").alias("TotalRevenue")
    ).orderBy(col("TotalRevenue").desc)
    dfRevenueByCategory.write.mode("overwrite").option("header", "true").csv("/app/output/aggregations/revenue_by_category")
    println("Total Revenue by Product Category saved to CSV.")


    // 2. Chiffre d'affaires par ville
    println("Calculating Total Revenue by City...")
    val dfRevenueByCity = dfFactCustomSales.join(
      dfDimGeography.select("GeographyKey", "City"),
      Seq("GeographyKey"),
      "inner"
    ).groupBy("City").agg(
      sum("LineTotal").alias("TotalRevenue")
    ).orderBy(col("TotalRevenue").desc)
    dfRevenueByCity.write.mode("overwrite").option("header", "true").csv("/app/output/aggregations/revenue_by_city")
    println("Total Revenue by City saved to CSV.")


    // 3. Chiffre d'affaires par année
    println("Calculating Total Revenue by Year...")
    val dfRevenueByYear = dfFactCustomSales.join(
      dfDimDate.select("DateKey", "CalendarYear"),
      dfFactCustomSales("DateKey") === dfDimDate("DateKey"),
      "inner"
    ).groupBy("CalendarYear").agg(
      sum("LineTotal").alias("TotalRevenue")
    ).orderBy(col("CalendarYear"))
    dfRevenueByYear.write.mode("overwrite").option("header", "true").csv("/app/output/aggregations/revenue_by_year")
    println("Total Revenue by Year saved to CSV.")


    println("\n--- Full ETL pipeline completed successfully! ---")

    // Arrêter la session Spark à la fin du script
    spark.stop()
  }
}