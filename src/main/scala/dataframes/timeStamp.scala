package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{month, year, years}

object timeStamp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-csv-test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema",  value = true)
      .csv("src/main/resources/timestampExample.csv")


//    df.select(month($"Date")).show()
//    df.select(year($"Date")).show()

    val df2 = df.withColumn("Year", year($"Date"));

    val df2avgs = df2.groupBy("Year").mean()

    df2avgs.show()
    df2avgs.select($"Year", $"avg(Value)".as("Close")).show()
  }

}
