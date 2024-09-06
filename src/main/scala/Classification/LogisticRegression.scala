package Classification

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.log4j._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession

object LogisticRegression {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val data = spark.read
      .option("inferSchema",value = true)
      .option("header", value = true)
      .format("csv")
      .load("src/main/scala/Classification/titanic.csv")

    data.printSchema()

    //grab data

    val logRegDataAll = (data.select($"Survived".as("label"),
                                  $"Pclass",$"Name",$"Sex",$"Age",$"SibSp",$"Parch",$"Fare",$"Embarked"))

    val logRegData = logRegDataAll.na.drop()


    // Converting Strings into numerical values
    val genderIndexer = new StringIndexer()
       .setInputCol("Sex")
       .setOutputCol("SexIndex")

    val embarkedIndexer = new StringIndexer()
      .setInputCol("Embarked")
      .setOutputCol("EmbarkedIndex")



    // Converiting Numberical Values into One Hot Encoding
    val genderEncoder = new OneHotEncoder()
      .setInputCol("SexIndex")
      .setOutputCol("SexVector")

    val embarkEncoder = new OneHotEncoder()
      .setInputCol("EmbarkedIndex")
      .setOutputCol("EmbarkedVector")

    val assembler = new VectorAssembler()
      .setInputCols(Array("Pclass","SexVector","Age","SibSp","Parch","Fare","EmbarkedVector"))
      .setOutputCol("features")

    val Array(training, test) = logRegData.drop($"Name").randomSplit(Array(0.7, 0.3), seed =  12345)

    val lr = new LogisticRegression()

    val pipeline = new Pipeline().setStages(Array(genderIndexer, embarkedIndexer, genderEncoder, embarkEncoder, assembler, lr))

    val model: PipelineModel = pipeline.fit(training)

    val result = model.transform(test)

    result.show(300)


    // Evaluador de precisión para problemas de clasificación
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(result)
    println(s"Accuracy = $accuracy")


    val predictionsAndLabels = result.select($"prediction", $"label").as[(Double, Double)].rdd

    // Crear el objeto de métricas
    val metrics = new MulticlassMetrics(predictionsAndLabels)

    // Obtener la matriz de confusión
    val confusionMatrix = metrics.confusionMatrix
    println("Confusion Matrix:")
    println(confusionMatrix)


    result.printSchema()



    //    val df = genderIndexer.fit(logRegData).transform(logRegData).select($"Sex", $"SexIndex")
//    assembler.transform(logRegDataAll).show()

//    genderEncoder.fit(df).transform(df).show()




  }

}
