import org.apache.spark.sql.functions.{collect_list, first, levenshtein, min}
import recommender.engine.DataProcessing.Udf.{convertPriceUdf, mapProviderUdf, normHotelNameUdf, sigmoidPriceUdf}

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields",100)
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix-tripi-dataprocessing")
  .getOrCreate()

import org.apache.spark.sql.functions.col
import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

val hotel_mapping = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_mapping", "keyspace" -> "testkeyspace"))
  .load()

val hotel_service = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_service", "keyspace" -> "testkeyspace"))
  .load()

val hotel_price_daily = spark.read
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_price_daily")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()

val root_hotel = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "root_hotel", "keyspace" -> "testkeyspace"))
  .load()

val hotel_price_daily_clean = hotel_price_daily.select(
  col("domain_id").cast("Int"),
  col("domain_hotel_id").cast("BigInt"),
  col("final_amount_min").cast("Float"),
  col("checkin_date_id").cast("Int"))

val hotel_mapping_with_price = hotel_mapping
  .join(hotel_price_daily_clean, Seq("domain_id", "domain_hotel_id"), "inner")
  .withColumn("price_score",sigmoidPriceUdf(col("final_amount_min")))

val mapping_with_service = hotel_mapping_with_price
  .join(hotel_service, Seq("hotel_id"), "inner")

val hotel_rank = mapping_with_service
  .withColumn("rank", col("service_score") / col("price_score"))

val hotel_rank_region = hotel_rank.filter(col("province_id").cast("Int") === 1)
val root_hotel_region = root_hotel.filter(col("province_id").cast("Int") === 1)

hotel_rank_region.show()

val hotel_rank_region_normName = hotel_rank_region.select(
  col("hotel_id"),
  col("domain_hotel_id"),
  col("domain_id"),
  col("rank"),
  col("url"),
  convertPriceUdf(col("final_amount_min")).as("final_amount_min"),
  normHotelNameUdf(col("name")).as("nameOther"),
  col("star_number").as("star_number_other"),
  col("overall_score").as("overall_score_other").cast("Float"),
  col("province"))
  .na.fill(0)


val root_hotel_region_normName = root_hotel_region.select(
  col("id").cast("String"),
  col("province_id"),
  normHotelNameUdf(col("name")).as("name"),
  col("address"),
  col("star_number"),
  col("overall_score"))
  .na.fill(0)

hotel_rank_region_normName.show()








