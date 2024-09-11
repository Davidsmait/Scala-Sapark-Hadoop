package Clustering

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{sum, udf}

import scala.collection.mutable.ArrayBuffer

/////////////////////////////////
// K MEANS PROJECT EXERCISE ////
///////////////////////////////

// Your task will be to try to cluster clients of a Wholesale Distributor
// based off of the sales of some product categories

// Source of the Data
//http://archive.ics.uci.edu/ml/datasets/Wholesale+customers

// Here is the info on the data:
// 1)	FRESH: annual spending (m.u.) on fresh products (Continuous);
// 2)	MILK: annual spending (m.u.) on milk products (Continuous);
// 3)	GROCERY: annual spending (m.u.)on grocery products (Continuous);
// 4)	FROZEN: annual spending (m.u.)on frozen products (Continuous)
// 5)	DETERGENTS_PAPER: annual spending (m.u.) on detergents and paper products (Continuous)
// 6)	DELICATESSEN: annual spending (m.u.)on and delicatessen products (Continuous);
// 7)	CHANNEL: customers Channel - Horeca (Hotel/Restaurant/Cafe) or Retail channel (Nominal)
// 8)	REGION: customers Region- Lisnon, Oporto or Other (Nominal)


////////////////////////////////////
// COMPLETE THE TASKS BELOW! //////
//////////////////////////////////
// Import SparkSession

// Optional: Use the following code below to set the Error reporting

// Create a Spark Session Instance

// Import Kmeans clustering Algorithm

// Load the Wholesale Customers Data

// Select the following columns for the training set:
// Fresh, Milk, Grocery, Frozen, Detergents_Paper, Delicassen
// Cal this new subset feature_data

// Import VectorAssembler and Vectors

// Create a new VectorAssembler object called assembler for the feature
// columns as the input Set the output column to be called features
// Remember there is no Label column

// Use the assembler object to transform the feature_data
// Call this new data training_data

// Create a Kmeans Model with K=3

// Fit that model to the training_data

// Evaluate clustering by computing Within Set Sum of Squared Errors.

// Shows the result.

object KMeans_Project_Exercise {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val data = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .format("csv")
      .load("src/main/scala/Clustering/Wholesale customers data.csv")

    val feature_data = data.select("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")

    val assembler = new VectorAssembler()
      .setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen"))
      .setOutputCol("features")

    val training_data = assembler.transform(feature_data)

    val kmeans = new KMeans().setK(5).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")


    val model = kmeans.fit(training_data)

    val predictions = model.transform(training_data)

    predictions.show(100)

    val evaluator = new ClusteringEvaluator()

    val silhoutte = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhoutte")

    println("Cluster Centers: ")
    val centroids = model.clusterCenters
    centroids.foreach(println)

    // Función para calcular la distancia cuadrada entre el punto y su centroide
    val distanceToCentroid = udf((prediction: Int, features: org.apache.spark.ml.linalg.Vector) => {
      Vectors.sqdist(features, centroids(prediction))
    })

    val predictionsWithDistance = predictions.withColumn("squaredDistance", distanceToCentroid($"prediction", $"features"))

    val WSS = predictionsWithDistance.agg(sum("squaredDistance")).first().getDouble(0)
    println(s"Within Set Sum of Squared Errors (WSS): $WSS")


    predictions.groupBy("prediction").count().show()


    var resultsFromElbowTest: List[(Int, Double)] = List()

    // Método del codo
    for (k <- 2 to 50) {
      val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("features")
      val model = kmeans.fit(training_data)
      val predictions = model.transform(training_data)

      // Calcular WSS manualmente
      val centroids = model.clusterCenters
      val distanceToCentroid = udf((prediction: Int, features: org.apache.spark.ml.linalg.Vector) => {
        Vectors.sqdist(features, centroids(prediction))
      })
      val predictionsWithDistance = predictions.withColumn("squaredDistance", distanceToCentroid($"prediction", $"features"))
      val WSS: Double = predictionsWithDistance.agg(sum("squaredDistance")).first().getDouble(0)

      resultsFromElbowTest = resultsFromElbowTest :+ (k, WSS)

      println(s"WSS for k=$k: $WSS")
    }

    resultsFromElbowTest.foreach { case (numero, texto) =>
      println(s"k: $numero, WSS: $texto")
    }


  }
}

