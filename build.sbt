name := "SPARK_Airbnb"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-hive" % "3.2.0"

)