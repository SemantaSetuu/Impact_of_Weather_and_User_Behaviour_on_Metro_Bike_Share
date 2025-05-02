ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BikeSharingDataForDISS_Project",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1" ,
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.1" ,
    //Dependency to enable Spark to interact with Azure Blob Storage using wasbs:// URLs
    libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.3.4",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.3.1"
  )
