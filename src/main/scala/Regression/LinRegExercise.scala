package Regression

////////////////////////////////////////////
//// LINEAR REGRESSION EXERCISE ///////////
/// Complete the commented tasks below ///
/////////////////////////////////////////

// Import LinearRegression

// Optional: Use the following code below to set the Error reporting
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

// Start a simple Spark Session

// Use Spark to read in the Ecommerce Customers csv file.

// Print the Schema of the DataFrame

////////////////////////////////////////////////////
//// Setting Up DataFrame for Machine Learning ////
//////////////////////////////////////////////////

// A few things we need to do before Spark can accept the data!
// It needs to be in the form of two columns
// ("label","features")

// Import VectorAssembler and Vectors

// Rename the Yearly Amount Spent Column as "label"
// Also grab only the numerical columns from the data
// Set all of this as a new dataframe called df

// An assembler converts the input values to a vector
// A vector is what the ML algorithm reads to train a model

// Use VectorAssembler to convert the input columns of data
// to a single output column of an array called "features"
// Set the input columns from which we are supposed to read the values.
// Call this new object assembler

// Use the assembler to transform our DataFrame to the two columns: label and features


// Create a Linear Regression Model object


// Fit the model to the data and call this model lrModel

// Print the coefficients and intercept for linear regression

// Summarize the model over the training set and print out some metrics!
// Use the .summary method off your model to create an object
// called trainingSummary

// Show the residuals, the RMSE, the MSE, and the R^2 Values.

object LinRegExercise {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("linRegExercise")
      .getOrCreate()
    import spark.implicits._

    val data = spark.read
      .option("header",value = true)
      .option("inferSchema",value = true)
      .csv("src/main/scala/Regression/Ecommerce.csv")

    data.printSchema()

    val colNames = data.columns
    val exampleRow = data.head(1)(0)

//    println(s"colNames: ${colNames.mkString(",")}")
    for (index <- 1 until colNames.length) {
      println(colNames(index))
      println(exampleRow(index))
      println("\n")
    }

    val df = data.select($"Yearly Amount Spent".as("label"),
                        $"Avg Session Length",
                        $"Time on App",
                        $"Time on Website",
                        $"Length of Membership")

    val assembler = new VectorAssembler()
      .setInputCols(Array("label", "Avg Session Length", "Time on App", "Time on Website", "Length of Membership"))
      .setOutputCol("features")

    df.show()

    val output = assembler.transform(df).select($"label", $"features")

    output.show()

    val lr = new LinearRegression()

    val lrModel = lr.fit(output)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")


    val trainingSummary =lrModel.summary

    trainingSummary.residuals.show(numRows = 10, truncate = false)


    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

  }
}
