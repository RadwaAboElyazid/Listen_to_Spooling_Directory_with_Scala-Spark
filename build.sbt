name := "scala-spark"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
)

// http://localhost:63342/scala-spark/log4j-1.2.17-javadoc.jar/org/apache/log4j/package-summary.html?_ijt=tfg2dhidbgfk66ei84gim8l500#package_description