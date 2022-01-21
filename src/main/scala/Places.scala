import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id}
import cities.Cities

case class Places(citiesListingDS: Cities[DataFrame]) {

  def run(): Unit = {
    val berlinNeighbourhoodsDS = citiesListingDS.berlin
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("neighbourhood_id", monotonically_increasing_id + 1)
      .withColumn("city", lit("Berlin"))
      .select("neighbourhood_id", "neighbourhood", "neighbourhood_group", "city")

    val parisNeighbourhoodsDS = citiesListingDS.paris
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("neighbourhood_id", monotonically_increasing_id + 1)
      .withColumn("city", lit("Paris"))
      .select("neighbourhood_id", "neighbourhood", "neighbourhood_group", "city")

    val madridNeighbourhoodsDS = citiesListingDS.madrid
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("neighbourhood_id", monotonically_increasing_id + 1)
      .withColumn("city", lit("Madrid"))
      .select("neighbourhood_id", "neighbourhood", "neighbourhood_group", "city")

    berlinNeighbourhoodsDS.write.insertInto("neighbourhoods")
    parisNeighbourhoodsDS.write.insertInto("neighbourhoods")
    madridNeighbourhoodsDS.write.insertInto("neighbourhoods")
  }
}
