import org.apache.spark.sql.SparkSession

object main {
  def main(args: Array[String]): Unit = {
    // Crear una SparkSession
    val spark = SparkSession.builder.appName("SimpleApp")
      .master("local[*]")
      .getOrCreate()

    // Crear un RDD desde una lista
    val data = Seq(("Alice", 34), ("Bob", 45), ("Cathy", 29))
    val rdd = spark.sparkContext.parallelize(data)

    // Transformación: Convertir el RDD en DataFrame
    import spark.implicits._
    val df = rdd.toDF("Name", "Age")

    // Acción: Mostrar el DataFrame
    df.show()

    // Terminar la sesión de Spark
    spark.stop()
  }
}
