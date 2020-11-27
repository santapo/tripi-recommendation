package recommender.engine.core

import java.util.Calendar

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import Udf._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Column

import scala.math.E


class DataProcessingNew {
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("tripi/5f1-Data Processing")
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

//    val hotel_location = spark.read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> "hotel_location", "keyspace" -> "testkeyspace"))
//      .load()

    val hotel_image = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_image")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
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
      avg(col("avg_price")).as("avg_price"),
      first(col("longitude")).as("longitude"),
      first(col("latitude")).as("latitude"),
      max(col("longitude_mapping")).as("longitude_mapping"),
      max(col("latitude_mapping")).as("latitude_mapping"),
      collect_list("suggest").as("suggest")
    )

    val mapping_root_agg_2nd = mapping_root_agg_1st
      .withColumn("map_long",mapLongitudeUdf(col("longitude"),col("longitude_mapping")))
      .withColumn("map_lat",mapLatitudeUdf(col("latitude"),col("latitude_mapping")))
      .filter(col("map_long")>102 && col("map_lat")>8)
      .filter(col("map_long")<115 && col("map_lat")<25)

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
      col("map_long").as("longitude"),
      col("map_lat").as("latitude"),
      col("suggest")
    )


    // key table
    val mapping_domain_hotel = mapping_root.select(
      col("table_id"),
      col("id"),
      col("hotel_id"),
      col("domain_id"),
      col("domain_hotel_id")
    )

    mapping_domain_hotel.createCassandraTable("testkeyspace","mapping_domain_hotel")
    mapping_domain_hotel
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_domain_hotel", "keyspace" -> "testkeyspace"))
      .save()

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
      max(col("sleep_quality_score")).as("sleep_quality_score"),
      max(col("meal_score")).as("meal_score"),
      max(col("location_score")).as("location_score"),
      max(col("service_score")).as("service_score"),
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

    // Mapping_location
    // val mapping_root_location = mapping_domain_hotel
    //  .join(hotel_location,Seq("hotel_id","domain_id"),"inner")




    // Mapping_review => add review count, image_list, review_list to mapping root final
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

    // Review count and review list
//
//    val hotel_review_with_text = spark.read
//      .format("jdbc")
//      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
//      .option("dbtable", "hotel_review")
//      .option("user", "FiveF1")
//      .option("password", "z3hE3TkjFzNyXhjb6iek")
//      .load()
//
//    val hotel_review_text = hotel_review_with_text.select(
//      col("id").cast("String").as("table_review_id"),
//      col("review_id").cast("Int"),
//      col("domain_id").cast("Int"),
//      col("domain_hotel_id").cast("BigInt"),
//      col("username").cast("String"),
//      col("text").cast("String"),
//      col("review_datetime").cast("Date"),
//      col("score").cast("Float")
//    )
//
//    val mapping_review_text = hotel_review_text
//      .join(mapping_domain_hotel,Seq("domain_id","domain_hotel_id"),"inner")

//    def limitSize(n: Int, arrCol: Column): Column =
//      array((0 until n).map(arrCol.getItem):_*)
//
//    val mapping_review_count_word = mapping_review_text
//      .withColumn("word_count",size(split(col("text")," ")))
//      .filter(col("word_count")>20)
//
//    val review_list = mapping_review_count_word
//      .withColumn("review_list",
//        mapReviewUdf(col("username"),col("domain_id"),col("text"),col("score"),col("review_datetime")))
//
//    val review_list_clean = review_list
//      .groupBy("id").agg(
//      collect_list(col("review_list")).as("review_list")
//    ).select(
//      col("id"),
//      limitSize(3,col("review_list")).as("review_list")
////    )
//    mapping_review_text.createCassandraTable("testkeyspace","mapping_review_text")
//    mapping_review_text
//      .write
//      .format("org.apache.spark.sql.cassandra")
//      .mode("Append")
//      .options(Map("table" -> "mapping_review_text", "keyspace" -> "testkeyspace"))
//      .save()
//    val mapping_review_list = mapping_hotel_review_clean
//      .withColumn("review_list",
//        mapReviewUdf(col("username"),col("domain_id"),col("text"),col("score"),col("review_datetime")))

//    val mapping_review_count = mapping_hotel_review_clean
//      .groupBy("id").agg(
//      count(lit(1)).as("review_count"),
//      collect_list(col("review_list")).as("review_list")
//    )

    val mapping_review_count = mapping_hotel_review_clean.groupBy("id").count()

    val mapping_root_clean_with_review_count = mapping_root_clean
      .join(mapping_review_count,Seq("id"),"inner")


    // Image list

    val mapping_image = hotel_image
      .join(mapping_domain_hotel,Seq("domain_id","domain_hotel_id"),"inner")

//    val mapping_image_list = mapping_image
//      .groupBy("id").agg(
//      collect_list(col("provider_url")).as("image_list")
//    ).select(
//      col("id"),
//      limitSize(10,col("image_list")).as("image_list")
//    )
    mapping_image.createCassandraTable("testkeyspace","mapping_image")
    mapping_image
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_image", "keyspace" -> "testkeyspace"))
      .save()

//    val mapping_root_review_image = mapping_root_clean_with_review_count

    val mapping_root_final = mapping_root_clean_with_review_count.select(
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
      col("count").as("review_count"),
      col("suggest")
    )

    mapping_root_final
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_root", "keyspace" -> "testkeyspace"))
      .save()


    // Mapping_logging
    val mapping_hotel_logging = hotel_logging
      .join(mapping_domain_hotel,Seq("id"),"inner")

    val mapping_hotel_logging_clean = mapping_hotel_logging.select(
      col("logging_id"),
      col("user_id"),
      col("session_id"),
      col("action_name"),
      col("id"),
      col("rank_on_page"),
      col("reviews_number"),
      col("star_number"),
      col("rating_level"),
      col("overall_score"),
      col("price")
    )

    mapping_hotel_logging_clean.createCassandraTable("testkeyspace","mapping_logging")
    mapping_hotel_logging_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_logging", "keyspace" -> "testkeyspace"))
      .save()

    println(Calendar.getInstance().getTime + ": Mapping process is success\n")
  }

  def dataClustering(): Unit = {
    println(Calendar.getInstance().getTime + ": Clustering with Kmeans\n")

    //
    // Read data from Cassandra
    //
    val mapping_root = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mapping_root", "keyspace" -> "testkeyspace"))
      .load()

    val mapping_review = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mapping_review", "keyspace" -> "testkeyspace"))
      .load()

    val mapping_service = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mapping_service", "keyspace" -> "testkeyspace"))
      .load()

    // Clustering
    val geolocation = mapping_root.select(
      col("id"),
      col("longitude"),
      col("latitude"),
      col("review_count")
    )

    val cols = Array("latitude","longitude")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    val featureDf = assembler.transform(geolocation)

    val kmeans = new KMeans()
      .setK(100)
      .setFeaturesCol("features")
      .setPredictionCol("hotel_cluster")

    val kmeansModel = kmeans.fit(featureDf)

    val cluster = kmeansModel.transform(featureDf)

    val cluster_clean = cluster.select(
      col("id"),
      col("hotel_cluster"),
      col("review_count")
    )

    // Add hotel_cluster to mapping_review -> mapping_review_score
    val mapping_review_with_cluster = mapping_review
      .join(cluster_clean,Seq("id"),"right")

    val new_review = mapping_review_with_cluster
      .withColumn("end_date",to_date(lit("2020-09-08")))
      .withColumn("date_distance",datediff(col("end_date"),col("review_datetime")))

    val review_quantity_cluster = mapping_review_with_cluster
      .groupBy("hotel_cluster").agg(
      avg(col("review_count")).as("avg_by_cluster")
    ).withColumn("Quantity", lit(1.44)*col("avg_by_cluster"))

    val review_score_1st = new_review
      .withColumn("score_1st_1",col("score")/col("date_distance"))
      .withColumn("score_1st_2",lit(10)/col("date_distance"))
      .groupBy("id").agg(
      first(col("hotel_cluster")).as("hotel_cluster"),
      sum(col("score_1st_1")).as("score_1st_1"),
      sum(col("score_1st_2")).as("score_1st_2"),
      count(col("id")).as("sum_review_by_id")
    ).join(review_quantity_cluster,Seq("hotel_cluster"),"inner")

    val review_score_2nd = review_score_1st
      .withColumn("final_review_score",
        lit(5)*col("score_1st_1")/col("score_1st_2")
          +lit(5)*(lit(1)-pow(lit(E),-col("sum_review_by_id")/col("Quantity"))))

    val mapping_review_score = review_score_2nd.select(
      col("id"),
      col("hotel_cluster"),
      col("final_review_score")
    )

    mapping_review_score.createCassandraTable("testkeyspace","mapping_review_score")
    mapping_review_score
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_review_score", "keyspace" -> "testkeyspace"))
      .save()

    // Add price/service score
    val mapping_price_with_cluster = mapping_root.select(
      col("id"),
      col("avg_price")
    ).join(cluster_clean,Seq("id"),"inner")

    val price_quantity_cluster = mapping_price_with_cluster
      .groupBy("hotel_cluster").agg(
      first(col("hotel_cluster")).as("hotel_cluster"),
      avg(col("avg_price")).as("avg_price_cluster")
    ).withColumn("Quantity",lit(1.44)*col("avg_price_cluster"))

    val price_score = mapping_price_with_cluster
      .join(price_quantity_cluster,Seq("hotel_cluster"),"inner")
      .withColumn("price_score",sigmoidPriceUdf(col("avg_price"),col("avg_price_cluster")))

    val service_score = mapping_service
      .withColumn("service_score",sigmoidServiceUdf(
        col("relax_spa"),
        col("relax_massage"),
        col("relax_outdoor_pool"),
        col("relax_sauna"),
        col("cleanliness_score"),
        col("meal_score")).cast("Float"))

    val service_per_price_score = price_score
      .join(service_score,Seq("id"),"inner")
      .withColumn("service_per_price_score",col("service_score")/col("price_score"))

    val service_per_price_score_clean = service_per_price_score.select(
      col("id"),
      col("price_score"),
      col("service_score"),
      col("service_per_price_score")
    )

    service_per_price_score_clean.createCassandraTable("testkeyspace","service_price_score")
    service_per_price_score_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "service_price_score", "keyspace" -> "testkeyspace"))
      .save()

    println(Calendar.getInstance().getTime + ": Clustering is Success\n")
  }



  def dataRankScore(): Unit = {
    println(Calendar.getInstance().getTime + ": Ranking...\n")
    //
    // Read data from Cassandra
    //
    val service_price_score = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "service_price_score", "keyspace" -> "testkeyspace"))
      .load()
    val mapping_review_score = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mapping_review_score", "keyspace" -> "testkeyspace"))
      .load()
    val mapping_root = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mapping_root", "keyspace" -> "testkeyspace"))
      .load()

    // FINAL SCORE
    val score = service_price_score
      .join(mapping_review_score,Seq("id"),"inner")
      .withColumn("Final_Score",col("final_review_score")+lit(7)*col("service_per_price_score"))
      .select(
        col("id"),
        col("hotel_cluster"),
        col("final_score")
      )

    val hotel_table = mapping_root
      .join(score,Seq("id"),"inner")

    val hotel_table_clean = hotel_table.select(
      col("id"),
      col("hotel_cluster"),
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
      col("review_count"),
      col("suggest"),
      col("final_score")
    )

    hotel_table
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_table", "keyspace" -> "testkeyspace"))
      .save()

    println(Calendar.getInstance().getTime + ": Ranking is Completed!\n")
  }

}
