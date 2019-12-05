name := "HedgeFundRealTimeAnalysis"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.2"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"
