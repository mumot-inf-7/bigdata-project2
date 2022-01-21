import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  var path: String = ""

  def main(args: Array[String]): Unit = {
    path = args(0)
    val ds = getDS()

    Facts().run()
    Times(ds.calendarsDS).run()
  }

  def getDS()= {
    val sqlContext = SparkSession.builder()
      .appName("Facts")
      .config("spark.master", "local")
      .getOrCreate()

    def getDF(fileName: String) = {
      sqlContext.read.format("org.apache.spark.csv")
        .option("header", true).option("inferSchema", true)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiline", true)
        .csv(path + "/" + fileName)
    }

    def readCsvFile(fileName: String) = {
      getDF(fileName)
        .cache()
    }

    val cities = List("Berlin", "Paris", "Madrid")

    val listingsDS = cities
      .map(city => readCsvFile(s"${city}Listings.csv"))
      .reduce((a, b) => a.union(b))

    val calendarsDS = List("Berlin", "Paris")
      .map(city => readCsvFile(s"${city}Calendar.csv"))
      .reduce((a, b) => a.union(b))
      .union(
        getDF("MadridCalendar.csv")
          .drop("adjusted_price", "minimum_nights", "maximum_nights")
      )

    object X{
      val calendarsDS: DataFrame = calendarsDS
      val listingsDS: DataFrame = listingsDS
    }

    X
  }
}
