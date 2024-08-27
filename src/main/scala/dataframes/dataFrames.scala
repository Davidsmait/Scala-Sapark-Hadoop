import org.apache.spark.sql.SparkSession

// Run this using commands
// spark-shell
// :load dataFrames.scala

//val sparkSession = SparkSession.builder().getOrCreate()
//import spark.implicits._
//val df = sparkSession.read.option("header", true).option("inferSchema", true).csv("datos.csv")
////df.head(5)
//df.show()
////df.describe().show()
////
////
////df.select("Nombre").show()
////
////val dfSum = df.withColumn("Edad de inicio", $"Edad" - $"Tiempo trabajando")
////
////dfSum.show()
//
//df.select($"Edad", $"Tiempo trabajando").filter($"Edad"> 30 && $"Tiempo trabajando" < 5).show()
//df.select($"Edad", $"Tiempo trabajando").filter("Edad > 30 AND `Tiempo trabajando` < 5").show()
//
//
//df.select(corr("Edad", "Tiempo trabajando")).show()
