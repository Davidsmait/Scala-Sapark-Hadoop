package Regression

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression


object linearRegressionExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder().master("local[*]").appName("linearRegressionExample").getOrCreate()
    import spark.implicits._

    val data = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .format("csv")
      .load("src/main/scala/Regression/Clean_USA_Housing.csv")
    data.printSchema()

    val df = (data.select($"Price".as("label"),
      $"Avg Area Income".cast("double"), $"Avg Area House Age".cast("double"), $"Avg Area Number of Rooms",
      $"Avg Area Number of Bedrooms",$"Area Population"))


    val assembler : VectorAssembler = (new VectorAssembler()
      .setInputCols(Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms","Area Population"))
      .setOutputCol("features"))



    val output = assembler.transform(df).select($"label", $"features")

    val lr = new LinearRegression()

    val lrModel = lr.fit(output)

    val trainingJobSummary = lrModel.summary

    trainingJobSummary.residuals.show()
    trainingJobSummary.predictions.show()

//    output.show()

  }
}


//def main(): Unit = {
//  Logger.getLogger("org").setLevel(Level.ERROR)
//
//
//  val spark = SparkSession.builder().appName("linearRegressionExample").getOrCreate()
//  import spark.implicits._
//
//  val data = spark.read
//    .option("header", value = true)
//    .option("inferSchema", value = true)
//    .format("csv")
//    .load("src/main/scala/Regression/USA_Housing.csv")
//
//  val df = (data.select($"Price".as("label"),
//    $"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms",
//    $"Avg Area Number of Bedrooms",$"Area Population"))
//
//
//  val assembler = (new VectorAssembler()
//    .setInputCols(Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms","Area Population"))
//    .setOutputCol("features"))
//
//  val output = assembler.transform(df).select($"label", $"features")
//
//  output.show()
//}
//main()



//    val colnames = data.columns
//    val firstrow = data.head(1)(0)
//    println(s"firstrow $firstrow")
//    println("\n")
//    println("Example Data Row")
//    for(ind <- Range(1,colnames.length)){
//      println(s"ind: $ind")
//      println(colnames(ind))
//      println(firstrow(ind))
//      println("\n")
//    }

// ("label", "features")
