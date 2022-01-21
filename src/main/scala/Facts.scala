import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Facts {
  def run(): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("Facts")
      .config("spark.master", "local")
      .getOrCreate()

    val berlinListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("BerlinListings.csv")
      .cache()

    val parisListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("ParisListings.csv")
      .cache()

    val madridListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("MadridListings.csv")
      .cache()

    val listingsDS = berlinListingsDS
      .union(parisListingsDS)
      .union(madridListingsDS)

    val berlinCalendarDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("BerlinCalendar.csv")
      .cache()

    val madridCalendarDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("MadridCalendar.csv")
      .drop("adjusted_price", "minimum_nights", "maximum_nights")
      .cache()

    val parisCalendarDS = sqlContext.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      option("quote", "\"").
      option("escape", "\"").
      option("multiline", true).
      csv("ParisCalendar.csv").cache()
  }
}
