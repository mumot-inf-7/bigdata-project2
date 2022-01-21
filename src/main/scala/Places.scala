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
      .cache()

    val parisNeighbourhoodsDS = citiesListingDS.paris
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("city", lit("Paris"))
      .select("neighbourhood", "neighbourhood_group", "city")
      .cache()

    val madridNeighbourhoodsDS = citiesListingDS.madrid
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("city", lit("Madrid"))
      .select("neighbourhood", "neighbourhood_group", "city")
      .cache()

    parisNeighbourhoodsDS
      .union(madridNeighbourhoodsDS)
      .union(berlinNeighbourhoodsDS)
      .withColumn("id", monotonically_increasing_id + 1)
      .select("id","neighbourhood", "neighbourhood_group", "city")
      .write
      .insertInto("places")
  }
}
