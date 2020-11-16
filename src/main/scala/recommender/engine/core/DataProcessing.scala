package recommender.engine.core

import java.util.Calendar

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import Udf._

class DataProcessing {

  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("phoenix-tripi-dataprocessing")
    .getOrCreate()

  import spark.implicits._

  val sparkContext = spark.sparkContext

  sparkContext.setLogLevel("WARN")

  val connector = CassandraConnector(sparkContext.getConf)
  connector.withSessionDo(session => {
        session.execute("USE testkeyspace")
  })

  def mapping(): Unit ={
    println(Calendar.getInstance().getTime + ": Start data mapping...\n")

    val hotel_price_daily = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_price_daily")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val hotel_price_daily_clean = hotel_price_daily.select(
      col("domain_id").cast("Int"),
      col("domain_hotel_id").cast("BigInt"),
      col("final_amount_min").cast("Float"),
      col("checkin_date_id").cast("Int"))

    val hotel_mapping = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "hotel_mapping", "keyspace" -> "testkeyspace"))
      .load()

    val hotel_service = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "hotel_service", "keyspace" -> "testkeyspace"))
      .load()

    val cosine_similar = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "cosine_similar", "keyspace" -> "testkeyspace"))
      .load()

    val root_hotel = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "root_hotel", "keyspace" -> "testkeyspace"))
      .load()

    val hotel_mapping_with_price = hotel_mapping
      .join(hotel_price_daily_clean, Seq("domain_id", "domain_hotel_id"), "inner")
      .withColumn("price_score",sigmoidPriceUdf(col("final_amount_min")))


    val mapping_with_service = hotel_mapping_with_price
      .join(hotel_service, Seq("hotel_id"), "inner")
      .withColumn("price_score", sigmoidPriceUdf(col("final_amount_min")))

    val hotel_rank = mapping_with_service
      .withColumn("rank", col("service_score") / col("price_score"))

    for(province_id <- 1 to 3) {

      val hotel_rank_region = hotel_rank.filter(col("province_id").cast("Int") === province_id)
      val root_hotel_region = root_hotel.filter(col("province_id").cast("Int") === province_id)

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

      val groupByName = root_hotel_region_normName
        .join(hotel_rank_region_normName, levenshtein(root_hotel_region_normName("name"), hotel_rank_region_normName("nameOther")) < 1)

      val groupById = groupByName.withColumn("suggest", mapProviderUdf(col("url"), col("domain_id"), col("final_amount_min")))

      val groupByIdPrice = groupById.groupBy("id").agg(
        first(col("domain_id")).as("provider"),
        first(col("province")).as("province"),
        first(col("name")).as("name"),
        first(col("rank")).as("rank"),
        first(col("address")).as("address"),
        first(col("star_number")).as("star_number"),
        first(col("overall_score")).as("overall_score"),
        min(col("final_amount_min")).as("price"),
        collect_list("suggest").as("suggest"))

      groupByIdPrice
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode("Append")
        .options(Map("table" -> "hotel_table", "keyspace" -> "testkeyspace"))
        .save()
      print(".")
    }
    print(Calendar.getInstance().getTime + ": Mapping process is success\n")
  }
}
