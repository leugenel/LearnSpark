name := "wordCount"

version := "1.0"

scalaVersion := "2.11.7"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "compile"
)
