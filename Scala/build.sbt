name := "Scala"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.3.0",
    "org.apache.spark" %% "spark-graphx" % "1.3.0",
    "javax.servlet" % "javax.servlet-api" % "3.1.0"
)

