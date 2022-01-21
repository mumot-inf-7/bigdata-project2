import cities.Cities
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id}

case class Hosts(citiesListingDS: Cities[DataFrame]) {

  def run(): Unit = {
    citiesListingDS.map((df, _) => df
      .select("host_id", "host_name")
      .distinct()
      .cache()
    )
      .toList()
      .reduce((a, b) => a.union(b))
      .withColumn("id", monotonically_increasing_id + 1)
      .withColumn("name", col("host_name"))
      .select("id", "name")
      .write
      .insertInto("hosts")
  }
}
