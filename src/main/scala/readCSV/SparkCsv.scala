package readCSV

import org.apache.spark.sql.SparkSession

object SparkCsv {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-csv-test")
      .master("local[*]")
      .config("spark.driver.bindAddress","192.168.0.47")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/AAPL.csv")

    df.show()
    df.printSchema()
  }

  }
