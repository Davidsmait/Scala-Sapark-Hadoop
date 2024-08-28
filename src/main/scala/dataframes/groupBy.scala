package dataframes

import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}

object groupBy {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-csv-test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema",  value = true)
      .csv("src/main/resources/known_companies_sales_data.csv")


    df.printSchema()

    val relationalGroupedDatasetCompany : RelationalGroupedDataset = df.groupBy("Company")

    relationalGroupedDatasetCompany.mean().show()
    relationalGroupedDatasetCompany.sum().show()
    relationalGroupedDatasetCompany.min().show()
    relationalGroupedDatasetCompany.max().show()

    df.show()
    df.orderBy($"Sales".desc).show()

  }

}
