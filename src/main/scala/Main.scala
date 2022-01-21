import org.apache.spark.sql.{DataFrame, SparkSession}
import cities._

object Main {
  var path: String = ""

  def main(args: Array[String]): Unit = {
    path = args(0)
    val ds = getDS()

    Times(ds.calendarDS.toList().reduce((a,b) => a.union(b))).run()
    Places(ds.listingsDS).run()
    Hosts(ds.listingsDS).run()
    Facts().run()

  }

  def getDS() = {
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

    val citiesListingsDS = CitiesBuilder.fromFn(city => readCsvFile(s"${city}Listings.csv"))

    val citiesCalendarsDS = CitiesBuilder.fromFn {
      case city@CityNames.Madrid => {
        getDF(s"${city}Calendar.csv")
          .drop("adjusted_price", "minimum_nights", "maximum_nights")
          .cache()
      }
      case city =>
        readCsvFile(s"${city}Calendar.csv")
    }


    case class X(calendarDS: Cities[DataFrame], listingsDS: Cities[DataFrame])
    X(citiesCalendarsDS, citiesListingsDS)
  }
}
