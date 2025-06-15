error id: file:///C:/Users/SALA/Desktop/projet%20final%20data%20Engeneer/etl/code%20scala/etl_transformation.scala:`<none>`.
file:///C:/Users/SALA/Desktop/projet%20final%20data%20Engeneer/etl/code%20scala/etl_transformation.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 92
uri: file:///C:/Users/SALA/Desktop/projet%20final%20data%20Engeneer/etl/code%20scala/etl_transformation.scala
text:
```scala

import org.apache.spark.sql.{SparkSession,Window}
import org.apache.spark.sql.functions._@@

import org.apache.spark.sql.types._

object AdventureWorksETL {
  def main(args: Array[String]): Unit = {
    // 1. Initialisation de la session Spark
    val spark = SparkSession.builder
      .appName("AdventureWorks ETL")
      .config("spark.jars", "/app/lib/mssql-jdbc-12.8.1.jre11.jar")
      .getOrCreate()

    // 2. Paramètres JDBC
    val jdbcUrl = "jdbc:sqlserver://sqlserver:1433;databaseName=AdventureWorks;encrypt=true;trustServerCertificate=true"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "SA")
    connectionProperties.put("password", "password123?")
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    println("Starting data extraction from AdventureWorks...")

    // --- 3. Extraction des tables sources ---
    println("Extracting Sales.Customer...")
    val dfCustomer = spark.read.jdbc(jdbcUrl, "Sales.Customer", connectionProperties)
    println("Sales.Customer extracted.")

    println("Extracting Person.Person...")
    val dfPerson = spark.read.jdbc(jdbcUrl, "Person.Person", connectionProperties)
    println("Person.Person extracted.")

    println("Extracting Person.PersonPhone...")
    val dfPersonPhone = spark.read.jdbc(jdbcUrl, "Person.PersonPhone", connectionProperties)
    println("Person.PersonPhone extracted.")

    println("Extracting Production.Product...")
    val dfProduct = spark.read.jdbc(jdbcUrl, "Production.Product", connectionProperties)
    println("Production.Product extracted.")

    println("Extracting Production.ProductCategory...")
    val dfProductCategory = spark.read.jdbc(jdbcUrl, "Production.ProductCategory", connectionProperties)
    println("Production.ProductCategory extracted.")

    println("Extracting Production.ProductSubcategory...")
    val dfProductSubcategory = spark.read.jdbc(jdbcUrl, "Production.ProductSubcategory", connectionProperties)
    println("Production.ProductSubcategory extracted.")

    println("Extracting Person.Address...")
    val dfAddress = spark.read.jdbc(jdbcUrl, "Person.Address", connectionProperties)
    println("Person.Address extracted.")

    println("Extracting Person.StateProvince...")
    val dfStateProvince = spark.read.jdbc(jdbcUrl, "Person.StateProvince", connectionProperties)
    println("Person.StateProvince extracted.")

    println("Extracting Person.CountryRegion...")
    val dfCountryRegion = spark.read.jdbc(jdbcUrl, "Person.CountryRegion", connectionProperties)
    println("Person.CountryRegion extracted.")

    println("Extracting Sales.SalesOrderHeader...")
    val dfSalesOrderHeader = spark.read.jdbc(jdbcUrl, "Sales.SalesOrderHeader", connectionProperties)
      .withColumnRenamed("ModifiedDate", "HeaderModifiedDate_Raw")
    println("Sales.SalesOrderHeader extracted.")

    println("Extracting Sales.SalesOrderDetail...")
    val dfSalesOrderDetail = spark.read.jdbc(jdbcUrl, "Sales.SalesOrderDetail", connectionProperties)
      .withColumnRenamed("ModifiedDate", "DetailModifiedDate_Raw")
    println("Sales.SalesOrderDetail extracted.")

    println("\n--- All source tables extracted. Starting Transformations ---")

    // --- 4. Transformation des Données ---

    // --- 4.1. DimCustomer ---
    println("\n--- Creating DimCustomer ---")

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

    println("DimCustomer created. Schema:")
    dfDimCustomer.printSchema()
    dfDimCustomer.show(5, false)

    dfDimCustomer.write.mode("overwrite").parquet("/app/output/dim_customer")
    println("DimCustomer saved to /app/output/dim_customer")


    // --- 4.2. DimProduct ---
    println("\n--- Creating DimProduct ---")

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

    println("DimProduct created. Schema:")
    dfDimProduct.printSchema()
    dfDimProduct.show(5, false)

    dfDimProduct.write.mode("overwrite").parquet("/app/output/dim_product")
    println("DimProduct saved to /app/output/dim_product")


    // --- 4.3. DimGeography ---
    println("\n--- Creating DimGeography ---")

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

    println("DimGeography created. Schema:")
    dfDimGeography.printSchema()
    dfDimGeography.show(5, false)

    dfDimGeography.write.mode("overwrite").parquet("/app/output/dim_geography")
    println("DimGeography saved to /app/output/dim_geography")


    // --- 4.4. DimDate ---
    println("\n--- Creating DimDate ---")

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

    println("DimDate created. Schema:")
    dfDimDate.printSchema()
    dfDimDate.show(5, false)

    dfDimDate.write.mode("overwrite").parquet("/app/output/dim_date")
    println("DimDate saved to /app/output/dim_date")


    // --- 4.5. FactCustomSales ---
    println("\n--- Creating FactCustomSales ---")

    val dfFactSalesTemp1 = dfSalesOrderHeader.join(
      dfSalesOrderDetail,
      dfSalesOrderHeader("SalesOrderID") === dfSalesOrderDetail("SalesOrderID"),
      "inner"
    )

    val dfFactSalesTemp2 = dfFactSalesTemp1.join(
      dfDimCustomer.select("CustomerKey", "CustomerID"),
      dfFactSalesTemp1("CustomerID") === dfDimCustomer("CustomerID"),
      "inner"
    )

    val dfFactSalesTemp3 = dfFactSalesTemp2.join(
      dfDimProduct.select("ProductKey", "ProductID"),
      dfFactSalesTemp2("ProductID") === dfDimProduct("ProductID"),
      "inner"
    )

    val dfFactSalesTemp4 = dfFactSalesTemp3.join(
      dfDimGeography.select("GeographyKey", "AddressID"),
      dfFactSalesTemp3("ShipToAddressID") === dfDimGeography("AddressID"),
      "left_outer"
    )

    val dfFactSalesTemp5 = dfFactSalesTemp4.withColumn("OrderDateKey",
      (year(col("OrderDate")) * 10000 + month(col("OrderDate")) * 100 + dayofmonth(col("OrderDate"))).cast(IntegerType)
    ).join(
      dfDimDate.select("DateKey", col("FullDateAlternateKey").alias("DimDateFullDateAlternateKey")),
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


    println("FactCustomSales created. Schema:")
    dfFactCustomSales.printSchema()
    dfFactCustomSales.show(5, false)

    dfFactCustomSales.write.mode("overwrite").parquet("/app/output/fact_custom_sales")
    println("FactCustomSales saved to /app/output/fact_custom_sales")

    println("\n--- All dimensions and fact table transformations completed and saved to Parquet! ---")

    // --- Performing Aggregations ---
    println("\n--- Performing Aggregations ---")

    // 1. Chiffre d'affaires par catégorie de produit
    println("\nTotal Revenue by Product Category:")
    val dfRevenueByCategory = dfFactCustomSales.join(
      dfDimProduct.select("ProductKey", "ProductCategoryName"),
      Seq("ProductKey"),
      "inner"
    ).groupBy("ProductCategoryName").agg(
      sum("LineTotal").alias("TotalRevenue")
    ).orderBy(col("TotalRevenue").desc)

    dfRevenueByCategory.show(false)

    // 2. Chiffre d'affaires par ville
    println("\nTotal Revenue by City:")
    val dfRevenueByCity = dfFactCustomSales.join(
      dfDimGeography.select("GeographyKey", "City"),
      Seq("GeographyKey"),
      "inner"
    ).groupBy("City").agg(
      sum("LineTotal").alias("TotalRevenue")
    ).orderBy(col("TotalRevenue").desc)

    dfRevenueByCity.show(false)

    // 3. Chiffre d'affaires par année
    println("\nTotal Revenue by Year:")
    val dfRevenueByYear = dfFactCustomSales.join(
      dfDimDate.select("DateKey", "CalendarYear"),
      dfFactCustomSales("DateKey") === dfDimDate("DateKey"),
      "inner"
    ).groupBy("CalendarYear").agg(
      sum("LineTotal").alias("TotalRevenue")
    ).orderBy(col("CalendarYear"))

    dfRevenueByYear.show(false)

    dfRevenueByCategory.write.mode("overwrite").option("header", "true").csv("/app/output/aggregations/revenue_by_category")
    dfRevenueByCity.write.mode("overwrite").option("header", "true").csv("/app/output/aggregations/revenue_by_city")
    dfRevenueByYear.write.mode("overwrite").option("header", "true").csv("/app/output/aggregations/revenue_by_year")


    // Arrêter la session Spark à la fin du script
    spark.stop()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.