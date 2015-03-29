name := "Scala"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0",
  "javax.servlet" % "javax.servlet-api" % "3.1.0"
)
    