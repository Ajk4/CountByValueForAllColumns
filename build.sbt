name := "Distinct"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"