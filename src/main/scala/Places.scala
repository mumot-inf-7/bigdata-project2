import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id}
import cities.Cities

case class Places(citiesListingDS: Cities[DataFrame]) {

  def run(): Unit = {
    val berlinNeighbourhoodsDS = citiesListingDS.berlin
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("city", lit("Berlin"))
      .select("neighbourhood", "neighbourhood_group", "city")

    val parisNeighbourhoodsDS = citiesListingDS.paris
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("city", lit("Paris"))
      .select("neighbourhood", "neighbourhood_group", "city")

    val madridNeighbourhoodsDS = citiesListingDS.madrid
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("city", lit("Madrid"))
      .select("neighbourhood", "neighbourhood_group", "city")

    parisNeighbourhoodsDS
      .union(madridNeighbourhoodsDS)
      .union(berlinNeighbourhoodsDS)
      .withColumn("id", monotonically_increasing_id + 1)
      .write
      .insertInto("places")
  }
}
