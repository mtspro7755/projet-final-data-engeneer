import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object AdventureWorksETL {
  def main(args: Array[String]): Unit = {
    // 1. Initialisation de la session Spark
    val spark = SparkSession.builder
      .appName("AdventureWorks ETL")
      .config("spark.jars", "/app/lib/mssql-jdbc-12.8.1.jre11.jar,/app/lib/postgresql-42.7.3.jar")
      .getOrCreate()

    import spark.implicits._

    // 2. Paramètres JDBC pour SQL Server (source)
    val jdbcUrlMsSql = "jdbc:sqlserver://sqlserver:1433;databaseName=AdventureWorks;encrypt=true;trustServerCertificate=true"
    val connectionPropertiesMsSql = new java.util.Properties()
    connectionPropertiesMsSql.put("user", "SA")
    connectionPropertiesMsSql.put("password", "password123?")
    connectionPropertiesMsSql.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    println("Starting data extraction from AdventureWorks...")

    // --- 3. Extraction des tables sources ---
    println("Extracting Sales.Customer...")
    val dfCustomer = spark.read.jdbc(jdbcUrlMsSql, "Sales.Customer", connectionPropertiesMsSql)
    println("Sales.Customer extracted.")

    println("Extracting Person.Person...")
    val dfPerson = spark.read.jdbc(jdbcUrlMsSql, "Person.Person", connectionPropertiesMsSql)
    println("Person.Person extracted.")

    println("Extracting Person.EmailAddress...")
    val dfEmailAddress = spark.read.jdbc(jdbcUrlMsSql, "Person.EmailAddress", connectionPropertiesMsSql)
    println("Person.EmailAddress extracted.")

    println("Extracting Sales.SalesOrderHeader...")
    val dfSalesOrderHeader = spark.read.jdbc(jdbcUrlMsSql, "Sales.SalesOrderHeader", connectionPropertiesMsSql)
    println("Sales.SalesOrderHeader extracted.")

    println("Extracting Sales.SalesOrderDetail...")
    val dfSalesOrderDetail = spark.read.jdbc(jdbcUrlMsSql, "Sales.SalesOrderDetail", connectionPropertiesMsSql)
    println("Sales.SalesOrderDetail extracted.")

    println("Extracting Production.Product...")
    val dfProduct = spark.read.jdbc(jdbcUrlMsSql, "Production.Product", connectionPropertiesMsSql)
    println("Production.Product extracted.")

    println("Extracting Production.ProductCategory...")
    val dfProductCategory = spark.read.jdbc(jdbcUrlMsSql, "Production.ProductCategory", connectionPropertiesMsSql)
    println("Production.ProductCategory extracted.")

    println("Extracting Production.ProductSubcategory...")
    val dfProductSubcategory = spark.read.jdbc(jdbcUrlMsSql, "Production.ProductSubcategory", connectionPropertiesMsSql)
    println("Production.ProductSubcategory extracted.")

    println("Extracting Person.Address...")
    val dfAddress = spark.read.jdbc(jdbcUrlMsSql, "Person.Address", connectionPropertiesMsSql)
    println("Person.Address extracted.")

    println("Extracting Person.StateProvince...")
    val dfStateProvince = spark.read.jdbc(jdbcUrlMsSql, "Person.StateProvince", connectionPropertiesMsSql)
    println("Person.StateProvince extracted.")

    println("Extracting Person.CountryRegion...")
    val dfCountryRegion = spark.read.jdbc(jdbcUrlMsSql, "Person.CountryRegion", connectionPropertiesMsSql)
    println("Person.CountryRegion extracted.")


    println("\n--- 4. Transformations et Création des Dimensions et de la table de Faits ---")

    // --- DimCustomer ---
    println("Creating DimCustomer...")
    val windowSpecCustomer = Window.orderBy($"CustomerID")
    // Création d'un DataFrame temporaire qui inclut CustomerID pour la jointure avec la table de faits
    val dfDimCustomerEnriched = dfCustomer.as("c")
      .join(dfPerson.as("p"), $"c.PersonID" === $"p.BusinessEntityID", "left")
      .join(dfEmailAddress.as("ea"), $"p.BusinessEntityID" === $"ea.BusinessEntityID", "left")
      .select(
        $"c.CustomerID", 
        row_number().over(windowSpecCustomer).cast(IntegerType).as("CustomerKey"),
        $"p.FirstName",
        $"p.LastName",
        $"ea.EmailAddress", 
        when($"c.StoreID".isNotNull, lit("Store")).otherwise(lit("Individual")).as("CustomerType")
      )

    // DataFrame final pour DimCustomer (selon le DDL)
    val dfDimCustomer = dfDimCustomerEnriched.select(
      $"CustomerKey",
      $"FirstName".cast(StringType).as("FirstName"),
      $"LastName".cast(StringType).as("LastName"),
      $"EmailAddress".cast(StringType).as("EmailAddress"),
      $"CustomerType".cast(StringType).as("CustomerType")
    )
    println("DimCustomer created.")
    dfDimCustomer.printSchema()
    dfDimCustomer.show(5)


    // --- DimProduct ---
    println("Creating DimProduct...")
    val windowSpecProduct = Window.orderBy($"ProductID")
    // Création d'un DataFrame temporaire qui inclut ProductID pour la jointure avec la table de faits
    val dfDimProductEnriched = dfProduct.as("p")
      .join(dfProductSubcategory.as("psc"), $"p.ProductSubcategoryID" === $"psc.ProductSubcategoryID", "left")
      .join(dfProductCategory.as("pc"), $"psc.ProductCategoryID" === $"pc.ProductCategoryID", "left")
      .select(
        $"p.ProductID", 
        row_number().over(windowSpecProduct).cast(IntegerType).as("ProductKey"),
        $"p.Name".as("ProductName"),
        $"p.ProductNumber",
        $"pc.Name".as("ProductCategory"),
        $"psc.Name".as("ProductSubcategory")
      )

    // DataFrame final pour DimProduct (selon le DDL)
    val dfDimProduct = dfDimProductEnriched.select(
      $"ProductKey",
      $"ProductName".cast(StringType).as("ProductName"),
      $"ProductNumber".cast(StringType).as("ProductNumber"),
      $"ProductCategory".cast(StringType).as("ProductCategory"),
      $"ProductSubcategory".cast(StringType).as("ProductSubcategory")
    )
    println("DimProduct created.")
    dfDimProduct.printSchema()
    dfDimProduct.show(5)


    // --- DimGeography ---
    println("Creating DimGeography...")
    val windowSpecGeography = Window.orderBy($"AddressID")
    // Création d'un DataFrame temporaire qui inclut AddressID pour la jointure avec la table de faits
    val dfDimGeographyEnriched = dfAddress.as("a")
      .join(dfStateProvince.as("sp"), $"a.StateProvinceID" === $"sp.StateProvinceID", "left")
      .join(dfCountryRegion.as("cr"), $"sp.CountryRegionCode" === $"cr.CountryRegionCode", "left")
      .select(
        $"a.AddressID", 
        row_number().over(windowSpecGeography).cast(IntegerType).as("GeographyKey"),
        $"a.City",
        $"sp.Name".as("StateProvince"), 
        $"cr.Name".as("CountryRegion"), 
        $"a.PostalCode"
      )

    // DataFrame final pour DimGeography (selon le DDL)
    val dfDimGeography = dfDimGeographyEnriched.select(
      $"GeographyKey",
      $"City".cast(StringType).as("City"),
      $"StateProvince".cast(StringType).as("StateProvince"),
      $"CountryRegion".cast(StringType).as("CountryRegion"),
      $"PostalCode".cast(StringType).as("PostalCode")
    )
    println("DimGeography created.")
    dfDimGeography.printSchema()
    dfDimGeography.show(5)


    // --- DimDate ---
    println("Creating DimDate...")
    // Créer un DataFrame de dates unique à partir de OrderDate
    val dfDates = dfSalesOrderHeader.select($"OrderDate".as("FullDate")).distinct()
    val windowSpecDate = Window.orderBy($"FullDate")

    // Création d'un DataFrame temporaire qui inclut FullDate pour la jointure avec la table de faits
    val dfDimDateEnriched = dfDates.select(
      $"FullDate", // Garder FullDate pour la jointure avec la table de faits
      (year($"FullDate") * 10000 + month($"FullDate") * 100 + dayofmonth($"FullDate")).cast(IntegerType).as("DateKey"),
      year($"FullDate").cast(IntegerType).as("Year"),
      month($"FullDate").cast(IntegerType).as("Month"),
      date_format($"FullDate", "MMMM").as("MonthName"),
      quarter($"FullDate").cast(IntegerType).as("Quarter")
    )

    // DataFrame final pour DimDate (selon le DDL)
    val dfDimDate = dfDimDateEnriched.select(
      $"DateKey",
      $"FullDate",
      $"Year",
      $"Month",
      $"MonthName".cast(StringType).as("MonthName"),
      $"Quarter"
    )
    println("DimDate created.")
    dfDimDate.printSchema()
    dfDimDate.show(5)


    // --- FactCustomSales ---
    println("Creating FactCustomSales...")
    val dfFactCustomSales = dfSalesOrderDetail.as("sod")
      .join(dfSalesOrderHeader.as("soh"), $"sod.SalesOrderID" === $"soh.SalesOrderID", "inner")
      .join(dfDimCustomerEnriched.as("dc"), $"soh.CustomerID" === $"dc.CustomerID", "inner") 
      .join(dfDimProductEnriched.as("dp"), $"sod.ProductID" === $"dp.ProductID", "inner")   
      .join(dfDimGeographyEnriched.as("dg"), $"soh.BillToAddressID" === $"dg.AddressID", "inner") 
      .join(dfDimDateEnriched.as("dd"), to_date($"soh.OrderDate") === $"dd.FullDate", "inner") 
      .select(
        $"dc.CustomerKey", 
        $"dp.ProductKey",
        $"dg.GeographyKey",
        $"dd.DateKey",
        $"sod.OrderQty".cast(ShortType).as("OrderQuantity"),
        $"sod.LineTotal".cast(DecimalType(19, 4)).as("SalesAmount"),
        $"sod.UnitPrice".cast(DecimalType(19, 4)).as("UnitPrice")
      )

    // Appliquer le schéma exact du DDL pour FactCustomSales
    val dfFactCustomSalesFinal = dfFactCustomSales.select(
      $"CustomerKey",
      $"ProductKey",
      $"GeographyKey",
      $"DateKey",
      $"OrderQuantity",
      $"SalesAmount",
      $"UnitPrice"
    )
    println("FactCustomSales created.")
    dfFactCustomSalesFinal.printSchema()
    dfFactCustomSalesFinal.show(5)


    // --- 5. Chargement des Données Transformées dans PostgreSQL ---
    println("\n--- Starting data loading into PostgreSQL ---")

    // Paramètres JDBC pour PostgreSQL (destination)
    val jdbcUrlPg = "jdbc:postgresql://host.docker.internal:5432/adventure_warehouse" 
    val connectionPropertiesPg = new java.util.Properties()
    connectionPropertiesPg.put("user", "postgres")
    connectionPropertiesPg.put("password", "pass") 
    connectionPropertiesPg.put("driver", "org.postgresql.Driver")
    connectionPropertiesPg.put("stringtype", "unspecified")

    // --- ORDRE DE CHARGEMENT MODIFIÉ POUR L'OVERWRITE AVEC CLÉS ÉTRANGÈRES ---

    // 1. Charger/Overwriter FactCustomSales en premier pour gérer les dépendances de clés étrangères
    println("Loading FactCustomSales to PostgreSQL (overwriting existing data)...")
    dfFactCustomSalesFinal.write
      .mode("overwrite")
      .jdbc(jdbcUrlPg, "public.\"FactCustomSales\"", connectionPropertiesPg)
    println("FactCustomSales loaded to PostgreSQL.")

    // 2. Ensuite, charger/Overwriter les tables de dimensions
 
    println("Loading DimDate to PostgreSQL (overwriting existing data)...")
    dfDimDate.write
      .mode("overwrite")
      .jdbc(jdbcUrlPg, "public.\"DimDate\"", connectionPropertiesPg)
    println("DimDate loaded to PostgreSQL.")

    println("Loading DimCustomer to PostgreSQL (overwriting existing data)...")
    dfDimCustomer.write
      .mode("overwrite")
      .jdbc(jdbcUrlPg, "public.\"DimCustomer\"", connectionPropertiesPg)
    println("DimCustomer loaded to PostgreSQL.")

    println("Loading DimProduct to PostgreSQL (overwriting existing data)...")
    dfDimProduct.write
      .mode("overwrite")
      .jdbc(jdbcUrlPg, "public.\"DimProduct\"", connectionPropertiesPg)
    println("DimProduct loaded to PostgreSQL.")

    println("Loading DimGeography to PostgreSQL (overwriting existing data)...")
    dfDimGeography.write
      .mode("overwrite")
      .jdbc(jdbcUrlPg, "public.\"DimGeography\"", connectionPropertiesPg)
    println("DimGeography loaded to PostgreSQL.")


    println("\n--- All data loaded into PostgreSQL. ---")

    // Arrêter la session Spark
    spark.stop()
    println("Spark session stopped.")
  }
}
