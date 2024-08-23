package readCSV

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkCsv {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-csv-test")
      .master("local[*]")
      .config("spark.driver.bindAddress","192.168.0.47")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/AAPL.csv")

    df.show()
    df.printSchema()
    import spark.implicits._

    // DSL DataFrame API
    df.select("Date", "Volume", "Close").show()

    // Ways to obtain a column
//    val column = df("Date")
//    col("Open")
//    $"Close"
//
//    df.select(df("Date"), col("Open"), $"Close").show()


    val openColumn = df("Open")
    val plusColumn = (openColumn + 2).as("OpenIncreasedBy2")
    val castColumn = openColumn.cast(StringType).as("")

    df.select(openColumn, plusColumn, castColumn).show()

  }

}
