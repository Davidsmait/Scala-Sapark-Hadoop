package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}

object missingData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-csv-test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema",  value = true)
      .csv("src/main/scala/dataframes/known_companies_sales_data_with_missing.csv")


    df.show()
    df.na.drop().show()
    df.na.drop(3).show()
    df.describe().show()
    val fillPerson: DataFrame = df.na.fill("New Person", Array("Person"))
    val fillCompany = fillPerson.na.fill("New Company", Array("Company"))
    fillCompany.na.fill(30982.25333333333, Array("Sales")).show()

  }

}
