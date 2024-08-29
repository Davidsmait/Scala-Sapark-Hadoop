package dataframes.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, corr, day, max, min, month, year}

// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

// Start a simple Spark Session
// Load the Netflix Stock CSV File, have Spark infer the data types.
// What are the column names?
// What does the Schema look like?
// Print out the first 5 rows.
// Use describe() to learn about the DataFrame.

// ??? Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.

// What day had the Peak High in Price?

// What is the mean of the Close column?

// What is the max and min of the Volume column?

// For Scala/Spark $ Syntax

// How many days was the Close lower than $ 600?

// What percentage of the time was the High greater than $500 ?

// What is the Pearson correlation between High and Volume?

// What is the max High per year?

// What is the average Close for each Calender Month?


object DataFrame_Project {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("project data frame")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/scala/dataframes/project/Netflix_2011_2016.csv")

    df.columns

    df.schema.names.foreach( name => {
      println("name: ", name)
    })

    df.printSchema()

    df.show(5)

    df.describe().show()

    val dfHVRatio = df.withColumn("HV Ratio", $"High" / $"Volume")

    dfHVRatio.show()

    val dfDateGruped = df.orderBy($"High".desc)
    dfDateGruped.select(day($"Date").as("day"), $"High").show(1)


    df.select(avg($"Close")).show()
    df.select(max($"Volume"), min($"Volume")).show()

    df.select(day($"Date"),$"Close")
      .filter($"Close" < 600)
      .show()

    df.filter($"High" > 500).show()

    df.select(corr($"High",$"Volume")).show()

    val dfYearGroup = df.groupBy("Date").max()

    dfYearGroup.select(year($"Date").as("year"), $"max(High)").show()

    val dfMonthGroup = df.groupBy(month($"Date").as("Month")).mean("Close")
    //    dfMonthGroup.select($"month(Date)".as("month"), $"avg(Close)").show()

    dfMonthGroup.orderBy("Month").show()
    dfMonthGroup.orderBy($"Month".desc).show()

  }


}
