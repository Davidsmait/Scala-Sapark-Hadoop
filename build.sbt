ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"
name := "ScalaApacheSpark"

// Define variables for versions
val sparkVersion = "3.5.1"
val hadoopVersion = "3.4.0"
val junitVersion = "5.9.2"
val mockitoVersion = "4.11.0"

libraryDependencies ++= Seq(
  // Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,


// Hadoop dependencies
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,

  // Other dependencies
  "com.google.guava" % "guava" % "31.1-jre",
  "org.apache.commons" % "commons-lang3" % "3.12.0",

  // Testing dependencies
  "org.junit.jupiter" % "junit-jupiter-api" % junitVersion % Test,
  "org.junit.jupiter" % "junit-jupiter-params" % junitVersion % Test,
  "org.junit.jupiter" % "junit-jupiter-engine" % junitVersion % Test,
  "org.assertj" % "assertj-core" % "3.24.1" % Test,
  "org.hamcrest" % "hamcrest-library" % "2.2" % Test,
  "org.mockito" % "mockito-core" % mockitoVersion % Test,
  "org.mockito" % "mockito-junit-jupiter" % mockitoVersion % Test
)

javacOptions ++= Seq(
  "-source", "11",
  "-target", "11"
)

testOptions in Test += Tests.Argument("-q", "-oD")
