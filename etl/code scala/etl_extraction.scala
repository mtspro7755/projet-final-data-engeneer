import org.apache.spark.sql.SparkSession
import java.util.Properties

object AdventureWorksETL {

  def main(args: Array[String]): Unit = {

    // Création de la session Spark
    val spark = SparkSession.builder()
      .appName("AdventureWorks ETL")
      .config("spark.jars", "/app/lib/mssql-jdbc-12.8.1.jre11.jar")
      .getOrCreate()

    // Paramètres JDBC
    val jdbcUrl = "jdbc:sqlserver://sqlserver:1433;databaseName=AdventureWorks;encrypt=true;trustServerCertificate=true"

    val connectionProperties = new Properties()
    connectionProperties.put("user", "SA")
    connectionProperties.put("password", "password123?")
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    // Fonction d'extraction + affichage
    def extractAndShow(tableName: String): Unit = {
      println(s"\nExtracting $tableName...")
      val df = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
      println(s"$tableName extracted. Schema:")
      df.printSchema()
      df.show(5)
    }

    // --- DimCustomer ---
    extractAndShow("Sales.Customer")
    extractAndShow("Person.Person")

    // --- DimProduct ---
    extractAndShow("Production.Product")
    extractAndShow("Production.ProductCategory")
    extractAndShow("Production.ProductSubcategory")

    // --- DimGeography ---
    extractAndShow("Person.Address")
    extractAndShow("Person.StateProvince")
    extractAndShow("Person.CountryRegion")

    // --- FactCustomSales ---
    extractAndShow("Sales.SalesOrderHeader")
    extractAndShow("Sales.SalesOrderDetail")

    println("\nAll required tables extracted successfully!")

    // Fermer la session
    spark.stop()
  }
}
