package Classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions.{col, hour}


//////////////////////////////////////////////
// LOGISTIC REGRESSION PROJECT //////////////
////////////////////////////////////////////

//  In this project we will be working with a fake advertising data set, indicating whether or not a particular internet user clicked on an Advertisement. We will try to create a model that will predict whether or not they will click on an ad based off the features of that user.
//  This data set contains the following features:
//    'Daily Time Spent on Site': consumer time on site in minutes
//    'Age': cutomer age in years
//    'Area Income': Avg. Income of geographical area of consumer
//    'Daily Internet Usage': Avg. minutes a day consumer is on the internet
//    'Ad Topic Line': Headline of the advertisement
//    'City': City of consumer
//    'Male': Whether or not consumer was male
//    'Country': Country of consumer
//    'Timestamp': Time at which consumer clicked on Ad or closed window
//    'Clicked on Ad': 0 or 1 indicated clicking on Ad

///////////////////////////////////////////
// COMPLETE THE COMMENTED TASKS BELOW ////
/////////////////////////////////////////

////////////////////////
/// GET THE DATA //////
//////////////////////

// Import SparkSession and Logistic Regression

// Optional: Use the following code below to set the Error reporting

// Create a Spark Session

// Use Spark to read in the Advertising csv file.

// Print the Schema of the DataFrame

///////////////////////
/// Display Data /////
/////////////////////

// Print out a sample row of the data (multiple ways to do this)

////////////////////////////////////////////////////
//// Setting Up DataFrame for Machine Learning ////
//////////////////////////////////////////////////

//   Do the Following:
//    - Rename the Clicked on Ad column to "label"
//    - Grab the following columns "Daily Time Spent on Site","Age","Area Income","Daily Internet Usage","Timestamp","Male"
//    - Create a new column called Hour from the Timestamp containing the Hour of the click

object LogRegProject {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val data = spark.read
      .option("inferSchema", value = true)
      .option("header", value = true)
      .format("csv")
      .load("src/main/scala/Classification/advertising.csv")

    data.printSchema()
    data.show()

    val columnNames = data.columns
    val firstCol = data.head(1)(0)

    for(index <- Range(0, firstCol.length)){
      println(columnNames(index))
      println(firstCol(index))
      println("\n")
    }

    val logRegDataAll = (data.select($"Clicked on Ad".as("label"),
      $"Daily Time Spent on Site",$"Age",$"Area Income",$"Daily Internet Usage", $"Country",$"Timestamp",$"Male", hour($"Timestamp").as("Hour")))


    val logRegDataCountry = data.select($"Clicked on Ad".as("label"), $"Country").na.drop()

    val countryIndexer = new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .setHandleInvalid("keep")


    val countryVector = new OneHotEncoder()
      .setInputCol("CountryIndex")
      .setOutputCol("CountryVector")

//    val dataCountryIndexer = countryIndexer.fit(logRegDataCountry).transform(logRegDataCountry)
//
//    countryVector.fit(dataCountryIndexer).transform(dataCountryIndexer).show()


    val assembler = new VectorAssembler()
      .setInputCols(Array("CountryIndex", "CountryVector"))
      .setOutputCol("features")

    val logisticRegression = new LogisticRegression()

    val pipeline = new Pipeline().setStages(Array(countryIndexer, countryVector, assembler, logisticRegression))

    val Array(training, test) = logRegDataCountry.randomSplit(Array(0.7, 0.3))

    val model = pipeline.fit(training)

    val result = model.transform(test)

    result.show()

    val predictionsAndLabels = result.select($"prediction", $"label").as[(Double, Double)].rdd

    // Crear el objeto de métricas
    val metrics = new MulticlassMetrics(predictionsAndLabels)

    // Obtener la matriz de confusión
    val confusionMatrix = metrics.confusionMatrix
    println("Confusion Matrix:")
    println(confusionMatrix)




//    val assembler = new VectorAssembler()
//      .setInputCols(Array())
//      .setOutputCol("features")
//
//    logRegDataAll.show()


  }
}
// Import VectorAssembler and Vectors

// Create a new VectorAssembler object called assembler for the feature
// columns as the input Set the output column to be called features


// Use randomSplit to create a train test split of 70/30


///////////////////////////////
// Set Up the Pipeline ///////
/////////////////////////////

// Import Pipeline
// Create a new LogisticRegression object called lr

// Create a new pipeline with the stages: assembler, lr

// Fit the pipeline to training set.


// Get Results on Test Set with transform

////////////////////////////////////
//// MODEL EVALUATION /////////////
//////////////////////////////////

// For Metrics and Evaluation import MulticlassMetrics

// Convert the test results to an RDD using .as and .rdd

// Instantiate a new MulticlassMetrics object

// Print out the Confusion matrix
