import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Facts {
  def read(path: String) = {

  }

  def run(path: String): Unit = {

    val sqlContext = SparkSession.builder()
      .appName("Facts")
      .config("spark.master", "local")
      .getOrCreate()

    def readCsvFile(fileName: String) = {
      sqlContext.read.format("org.apache.spark.csv")
        .option("header", true).option("inferSchema", true)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiline", true)
        .csv(path + "/" + fileName)
        .cache()
    }

    val cities = List("Berlin", "Paris", "Madrid")

    val listingsDS = cities
      .map(city => readCsvFile(s"${city}Listings.csv"))
      .reduce((a, b) => a.union(b))

    val calendarsDS = cities
      .map(city => readCsvFile(s"${city}Calendar.csv"))
      .reduce((a, b) => a.union(b))

  }
}
