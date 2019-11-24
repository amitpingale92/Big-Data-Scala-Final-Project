name := "Project"

version := "0.1"

scalaVersion := "2.12.0"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"
)