name := "AdventureWorksETL"
version := "1.0"
scalaVersion := "2.12.18" // Assurez-vous que c'est compatible avec votre version de Spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided" // Utilisez la version de Spark de votre environnement