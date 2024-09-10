package Model_Validation

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

object Example {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val data = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .format("csv")
      .load("src/main/scala/Regression/Clean_USA_Housing.csv")

    val df = data.select($"Price".as("label"),
      $"Avg Area Income",$"Avg Area House Age",$"Avg Area Number of Rooms",$"Avg Area Number of Bedrooms", $"Area Population")


    val assembler = new VectorAssembler()
      .setInputCols(Array("Avg Area Income","Avg Area House Age","Avg Area Number of Rooms","Avg Area Number of Bedrooms", "Area Population"))
      .setOutputCol("features")


    val output = assembler.transform(df).select("label", "features")


    // Training and test data

    val Array(training, test) = output.randomSplit(Array(0.7, 0.3), seed = 12345)

    val lr = new LinearRegression()

    //Parameter grid builder

    val paramGridBuilder = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.2))
      .build()

    //Train split (holdout)

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
      .setEstimatorParamMaps(paramGridBuilder)
      .setTrainRatio(0.9)

    val model = trainValidationSplit.fit(training)

    model.transform(test).show()

    println(model.validationMetrics.mkString("Array(", ", ", ")"))


  }

}
