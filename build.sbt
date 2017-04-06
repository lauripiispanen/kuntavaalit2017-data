name := "Kuntavaalidata 2017"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"

mainClass in (Compile, run) := Some("io.github.lauripiispanen.kuntavaalidata.KuntavaalidataApp")
