import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, dayofweek, dayofmonth, monotonically_increasing_id, month, quarter, weekofyear, year}

case class Times(calendarDS: DataFrame) {
  def run(): Unit = {

    val timesDS = calendarDS.select("date")
      .distinct()
      .withColumn("date", col("date"))
      .withColumn("day", dayofmonth(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("year", year(col("date")))
      .select("date_id", "date", "day", "month", "year")

    timesDS.write.insertInto("times")
  }
}