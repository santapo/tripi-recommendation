package recommender.engine.core

import java.util.Calendar

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import Udf._

class DataProcessingNew {
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

  def dataMapping(): Unit = {
    println(Calendar.getInstance().getTime + ": Start data mapping...\n")

    //
    // Read data from Cassandra
    //
    val hotel_mapping = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "hotel_mapping", "keyspace" -> "testkeyspace"))
      .load()

    val cosine_similar = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "cosine_similar", "keyspace" -> "testkeyspace"))
      .load()

    val root_hotel = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "root_hotel", "keyspace" -> "testkeyspace"))
      .load()

    val hotel_service = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "hotel_service", "keyspace" -> "testkeyspace"))
      .load()

    val hotel_review = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "hotel_review", "keyspace" -> "testkeyspace"))
      .load()

    val hotel_logging = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "hotel_logging", "keyspace" -> "testkeyspace"))
      .load()

    // Mapping_root => Mapping_root_group_id (contain available hotel_domain)
    val mapping_root = cosine_similar
      .join(hotel_mapping,Seq("domain_id","domain_hotel_id"),"inner")
      .join(root_hotel,Seq("id"),"inner")

    val mapping_root_suggest = mapping_root
      .withColumn("suggest",mapProviderUdf(col("url"), col("domain_id"), col("avg_price")))

    val mapping_root_agg_1st = mapping_root_suggest
      .groupBy("id").agg(
      first(col("name")).as("name"),
      first(col("address")).as("address"),
      first(col("logo")).as("logo"),
      first(col("star_number")).as("star_number"),
      first(col("checkin_time")).as("checkin_time"),
      first(col("checkout_time")).as("checkout_time"),
      first(col("overall_score")).as("overall_score"),
      first(col("description")).as("description"),
      min(col("avg_price")).as("avg_price"),
      first(col("longitude")).as("longitude"),
      first(col("latitude")).as("latitude"),
      avg(col("longitude_mapping")).as("longitude_mapping"),
      avg(col("latitude_mapping")).as("latitude_mapping"),
      collect_list("suggest").as("suggest")
    )

    val mapping_root_agg_2nd = mapping_root_agg_1st
      .withColumn("map_long",mapLongitudeUdf(col("longitude"),col("longitude_mapping")))
      .withColumn("map_lat",mapLatitudeUdf(col("latitude"),col("latitude_mapping")))

    val mapping_root_clean = mapping_root_agg_2nd.select(
      col("id"),
      col("name"),
      col("address"),
      col("logo"),
      col("star_number"),
      col("checkin_time"),
      col("checkout_time"),
      col("overall_score"),
      col("description"),
      col("avg_price"),
      col("longitude"),
      col("latitude"),
      col("suggest")
    )

    mapping_root_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_root", "keyspace" -> "testkeyspace"))
      .save()

    // key table
    val mapping_domain_hotel = mapping_root.select(
      col("table_id"),
      col("id"),
      col("hotel_id"),
      col("domain_id"),
      col("domain_hotel_id")
    )

    // Mapping_service
    val mapping_root_service = mapping_domain_hotel
      .join(hotel_service,Seq("hotel_id"),"inner")
      .filter(col("hotel_id") isNotNull)

    val mapping_service = mapping_root_service
      .groupBy("id").agg(
      max(col("tours")).as("tour"),
      max(col("night_club")).as("night_club"),
      max(col("relax_spa")).as("relax_spa"),
      max(col("relax_massage")).as("relax_massage"),
      max(col("relax_steam_room")).as("relax_steam_room"),
      max(col("relax_outdoor_pool")).as("relax_outdoor_pool"),
      max(col("relax_sauna")).as("relax_sauna"),
      max(col("cleanliness_score")).as("cleanliness_score"),
      max(col("meal_score")).as("meal_score"),
      max(col("location_score")).as("location_score"),
      max(col("sleep_quality_score")).as("sleep_quality_score"),
      max(col("room_score")).as("room_score"),
      max(col("service_score")).as("service_score"),
      max(col("facility_score")).as("facility_score"),
      max(col("currency_exchange")).as("currency_exchange"),
      max(col("room_service_24_hour")).as("room_service_24_hour"),
      max(col("elevator")).as("elevator"),
      max(col("safely_deposit_boxed")).as("safely_deposit_boxed"),
      max(col("luggage_storage")).as("luggage_storage"),
      max(col("poolside_bar")).as("poolside_bar"),
      max(col("airport_transfer")).as("airport_transfer"),
      max(col("restaurants")).as("restaurants"),
      max(col("concierge")).as("concierge"),
      max(col("shops")).as("shops"),
      max(col("meeting_facilities")).as("meeting_facilities"),
      max(col("baby_sitting")).as("baby_sitting"),
      max(col("facilities_for_disabled_guests")).as("facilities_for_disabled_guests"),
      max(col("private_beach")).as("private_beach"),
      max(col("front_desk_24_hour")).as("front_desk_24_hour"),
      max(col("bar")).as("bar"),
      max(col("laundry_service")).as("laundry_service"),
      max(col("shuttle_room")).as("shuttle_room")
    )

    mapping_service.createCassandraTable("testkeyspace","mapping_service")
    mapping_service
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_service", "keyspace" -> "testkeyspace"))
      .save()

    // Mapping_review
    val mapping_hotel_review = hotel_review
      .join(mapping_domain_hotel,Seq("domain_id","domain_hotel_id"),"inner")

    val mapping_hotel_review_clean = mapping_hotel_review.select(
      col("table_review_id"),
      col("domain_id"),
      col("domain_hotel_id"),
      col("id"),
      col("review_datetime"),
      col("score")
    )

    mapping_hotel_review_clean.createCassandraTable("testkeyspace","mapping_review")
    mapping_hotel_review_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_review", "keyspace" -> "testkeyspace"))
      .save()

    // Mapping_logging
    val mapping_hotel_logging = hotel_logging
      .join(mapping_domain_hotel,Seq("id"),"inner")

    val mapping_hotel_logging_clean = mapping_hotel_logging.select(
      col("logging_id"),
      col("user_id"),
      col("action_name"),
      col("id"),
      col("room_night"),
      col("adult_num"),
      col("rank_on_page")
    )

    mapping_hotel_logging_clean.createCassandraTable("testkeyspace","mapping_logging")
    mapping_hotel_logging_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_logging", "keyspace" -> "testkeyspace"))
      .save()

    print(Calendar.getInstance().getTime + ": Mapping process is success\n")
  }
}