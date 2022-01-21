import cities.Cities
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id}

case class Hosts(citiesListingDS: Cities[DataFrame]) {

  def run() = {
    citiesListingDS.map((df, city) => df
      .select("host_id", "host_name")
      .distinct()
      .withColumn("id", col("host_id"))
      .cache()
    )
      .toList()
      .reduce((a, b) => a.union(b))
      .withColumn("name", col("host_name"))
      .select("id", "name")
      .cache()
  }
}
