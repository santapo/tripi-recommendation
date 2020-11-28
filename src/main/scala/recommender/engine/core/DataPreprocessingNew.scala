package recommender.engine.core

import java.util.Calendar

import akka.actor.Actor
import Udf._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.commons.lang3.StringUtils.stripAccents

class DataPreprocessingNew {
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("tripi/5f1-Data Preprocessing")
    .getOrCreate()

  import spark.implicits._

  val sparkContext = spark.sparkContext

  sparkContext.setLogLevel("WARN")

  val connector = CassandraConnector(sparkContext.getConf)
  connector.withSessionDo(session => {
    session.execute("DROP KEYSPACE IF EXISTS testkeyspace")
    session.execute("CREATE KEYSPACE testkeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute("USE testkeyspace")
    session.execute("CREATE TABLE testkeyspace.mapping_root " +
      "(id text PRIMARY KEY," +
      " name text," +
      " address text," +
      " province_name text," +
      " district_name text," +
      " street_name text," +
      " logo text," +
      " star_number int," +
      " checkin_time text," +
      " checkout_time text," +
      " overall_score float," +
      " description text," +
      " avg_price float," +
      " longitude float," +
      " latitude float," +
      " review_count int," +
      " suggest list<frozen <map<text,text>>>)")

    session.execute("CREATE TABLE testkeyspace.hotel_table " +
      "(id text PRIMARY KEY," +
      " hotel_cluster int," +
      " name text," +
      " address text," +
      " province_name text," +
      " district_name text," +
      " street_name text," +
      " logo text," +
      " star_number int," +
      " checkin_time text," +
      " checkout_time text," +
      " overall_score float," +
      " description text," +
      " avg_price float," +
      " longitude float," +
      " latitude float," +
      " review_count int," +
      " suggest list<frozen <map<text,text>>>," +
      " final_score Double)")

//    session.execute("CREATE TABLE testkeyspace.mapping_review_list " +
//      "(id text PRIMARY KEY," +
//      " review_list list<frozen <map<text,text>>>)")
//
//    session.execute("CREATE TABLE testkeyspace.mapping_image_list " +
//      "(id text PRIMARY KEY," +
//      " image_list list<text>)")

  }
  )

  val dataProcess = new DataProcessingNew

  def dataToCassandra(): Unit = {

    /**
     * Read data from Clickhouse Database then save in Cassandra
     **/
    println(Calendar.getInstance().getTime + ": Data is Saving to Cassandra... \n")

    //
    // Read data from Clickhouse
    //
    val cosine_hotel = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "cosine_hotel")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val hotel_mapping = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_mapping")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val roothotel_info = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "roothotel_info")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val hotel_quality = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_quality")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val hotel_facility = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_facility")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val hotel_service = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_service")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val hotel_review = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_review")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val hotel_logging = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_logging")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()


    val hotel_price_daily = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_price_daily")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

//    val hotel_location = spark.read
//      .format("jdbc")
//      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
//      .option("dbtable", "hotel_location")
//      .option("user", "FiveF1")
//      .option("password", "z3hE3TkjFzNyXhjb6iek")
//      .load()
//
//    val hotel_distance_to_location = spark.read
//      .format("jdbc")
//      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
//      .option("dbtable", "hotel_distance_to_location")
//      .option("user", "FiveF1")
//      .option("password", "z3hE3TkjFzNyXhjb6iek")
//      .load()

//    val domain = spark.read
//      .format("jdbc")
//      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
//      .option("dbtable", "domain")
//      .option("user", "FiveF1")
//      .option("password", "z3hE3TkjFzNyXhjb6iek")
//      .load()

    val province = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "province")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val district = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "district")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val street = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "street")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    //
    // Preprocess then save in Cassandra
    //

    // Create hotel_mapping table with max min avg price
    val hotel_price_daily_clean = hotel_price_daily.select(
      col("domain_id").cast("Int"),
      col("domain_hotel_id").cast("BigInt"),
      col("final_amount_min").cast("Float"))

    val price_min_max = hotel_price_daily_clean
      .groupBy("domain_id","domain_hotel_id")
      .agg(avg("final_amount_min").as("avg_price"))

    val hotel_mapping_clean = hotel_mapping.select(
      col("id").cast("String"),
      col("domain_id").cast("Int"),
      col("domain_hotel_id").cast("BigInt"),
      col("name").cast("String"),
      col("url").cast("String"),
      col("longitude").cast("Float"),
      col("latitude").cast("Float"),
      col("address").cast("String"),
      col("star_number").cast("Int"),
      col("overall_score").cast("Float"),
      col("checkin_time").cast("String"),
      col("checkout_time").cast("String"))
      .filter(col("url") =!= "")
      .filter(col("address") isNotNull)
      .dropDuplicates("domain_id", "domain_hotel_id")

    val hotel_mapping_with_price = hotel_mapping_clean
      .join(price_min_max,Seq("domain_id","domain_hotel_id"),"inner")

    val hotel_mapping_with_price_clean = hotel_mapping_with_price.select(
      col("id").cast("String").as("hotel_id"),
      col("domain_id").cast("Int"),
      col("domain_hotel_id").cast("BigInt"),
      col("name").cast("String").as("name_mapping"),
      col("address").cast("String").as("address_mapping"),
      col("url").cast("String"),
      col("longitude").cast("Float").as("longitude_mapping"),
      col("latitude").cast("Float").as("latitude_mapping"),
      col("star_number").cast("Int").as("star_mapping"),
      col("overall_score").cast("Float").as("overall_mapping"),
      col("checkin_time").cast("String").as("checkin_mapping"),
      col("checkout_time").cast("String").as("checkout_mapping"),
      col("avg_price").cast("Float")
    )

    hotel_mapping_with_price_clean.createCassandraTable("testkeyspace", "hotel_mapping")
    hotel_mapping_with_price_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_mapping", "keyspace" -> "testkeyspace"))
      .save()

    // Create hotel_service table with service_score
    val hotel_service_clean = hotel_service.select(
      col("hotel_id").cast("String"),
      col("tours").cast("Int"),
      col("night_club").cast("Int"),
      col("relax_spa").cast("Int"),
      col("relax_massage").cast("Int"),
      col("relax_steam_room").cast("Int"),
      col("relax_outdoor_pool").cast("Int"),
      col("relax_sauna").cast("Int"))

    val hotel_quality_clean = hotel_quality.select(
      col("hotel_id").cast("String"),
      col("cleanliness_score").cast("Float"),
      col("meal_score").cast("Float"),
      col("location_score").cast("Float"),
      col("sleep_quality_score").cast("Float"),
      col("room_score").cast("Float"),
      col("service_score").cast("Float"),
      col("facility_score").cast("Float")
    )

    val hotel_facility_clean = hotel_facility.select(
      col("hotel_id").cast("String"),
      col("currency_exchange").cast("Int"),
      col("room_service_24_hour").cast("Int"),
      col("elevator").cast("Int"),
      col("safely_deposit_boxed").cast("Int"),
      col("luggage_storage").cast("Int"),
      col("poolside_bar").cast("Int"),
      col("airport_transfer").cast("Int"),
      col("restaurants").cast("Int"),
      col("concierge").cast("Int"),
      col("shops").cast("Int"),
      col("meeting_facilities").cast("Int"),
      col("baby_sitting").cast("Int"),
      col("facilities_for_disabled_guests").cast("Int"),
      col("private_beach").cast("Int"),
      col("front_desk_24_hour").cast("Int"),
      col("bar").cast("Int"),
      col("laundry_service").cast("Int"),
      col("shuttle_room").cast("Int"))

    val hotel_service_table = hotel_quality_clean
      .join(hotel_facility_clean, Seq("hotel_id"), "inner")
      .join(hotel_service_clean, Seq("hotel_id"), "inner")
      .dropDuplicates()

    hotel_service_table.createCassandraTable("testkeyspace", "hotel_service")
    hotel_service_table
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_service", "keyspace" -> "testkeyspace"))
      .save()


    // Clean root_hotel table
    val roothotel_info_clean = roothotel_info.select(
      col("id").cast("Int"),
      col("name").cast("String"),
      col("address").cast("String"),
      col("logo").cast("String"),
      col("longitude").cast("Float"),
      col("latitude").cast("Float"),
      col("star_number").cast("Int"),
      col("checkin_time").cast("String"),
      col("checkout_time").cast("String"),
      col("overall_score").cast("Float"),
      col("description").cast("String"),
      col("province_id").cast("Int"),
      col("district_id").cast("Int"),
      col("street_id").cast("Int"))

    // Province, Street, District table

    val province_clean = province.select(
      col("id").cast("Int").as("province_id"),
      col("name").cast("String").as("province_name")
    )

    val district_clean = district.select(
      col("id").cast("Int").as("district_id"),
      col("province_id").cast("Int"),
      col("name").cast("String").as("district_name")
    )

    val street_clean = street.select(
        col("id").cast("Int").as("street_id"),
        col("district_id").cast("Int"),
        col("province_id").cast("Int"),
        col("name").cast("String").as("street_name")
      )

//    val stringNormalizer = udf((s: String) => stripAccents(s))
//    val street_clean = street.withColumn("name_no_accent",stringNormalizer(lower(col("name"))))
//      .select(
//        col("id").cast("Int").as("street_id"),
//        col("district_id").cast("Int"),
//        col("province_id").cast("Int"),
//        col("name_no_accent").cast("String").as("street_name")
//      )

    val roothotel_final = roothotel_info_clean
      .join(province_clean,Seq("province_id"),"inner")
      .join(district_clean,Seq("district_id","province_id"),"left")
      .join(street_clean,Seq("province_id","district_id","street_id"),"left")

    val roothotel_final_clean = roothotel_final.select(
      col("id").cast("Int"),
      col("name").cast("String"),
      col("address").cast("String"),
      col("logo").cast("String"),
      col("longitude").cast("Float"),
      col("latitude").cast("Float"),
      col("star_number").cast("Int"),
      col("checkin_time").cast("String"),
      col("checkout_time").cast("String"),
      col("overall_score").cast("Float"),
      col("description").cast("String"),
      col("province_name").cast("String"),
      col("district_name").cast("String"),
      col("street_name").cast("String")
    )

    roothotel_final_clean.createCassandraTable("testkeyspace", "root_hotel")
    roothotel_final_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "root_hotel", "keyspace" -> "testkeyspace"))
      .save()


    // clean review and logging
    val hotel_review_clean = hotel_review.select(
      col("id").cast("String").as("table_review_id"),
      col("review_id").cast("Int"),
      col("domain_id").cast("Int"),
      col("domain_hotel_id").cast("BigInt"),
      col("review_datetime").cast("Date"),
      col("score").cast("Float")
    )

    hotel_review_clean.createCassandraTable("testkeyspace","hotel_review")
    hotel_review_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_review", "keyspace" -> "testkeyspace"))
      .save()

    val hotel_logging_clean = hotel_logging.select(
      col("id").cast("String").as("logging_id"),
      col("user_id").cast("BigInt"),
      col("session_id").cast("BigInt"),
      col("action_name").cast("String"),
      col("hotel_id").cast("Int").as("id"),
      col("rank_on_page").cast("Int"),
      col("reviews_number").cast("Int"),
      col("star_number").cast("Int"),
      col("rating_level").cast("Int"),
      col("overall_score").cast("Float"),
      col("price").cast("Int")
    )

    hotel_logging_clean.createCassandraTable("testkeyspace","hotel_logging")
    hotel_logging_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_logging", "keyspace" -> "testkeyspace"))
      .save()

    // Clean hotel location
//    val hotel_location_clean = hotel_location.select(
//      col("id").cast("String").as("location_id"),
//      col("domain_id").cast("Int"),
//      col("hotel_id").cast("String")
//    )
//
//    val hotel_mapping_location_distance = hotel_distance_to_location
//      .join(hotel_location_clean,Seq("location_id","domain_id"),"inner")
//      .select(
//        col("id").cast("String").as("table_id"),
//        col("domain_id"),
//        col("hotel_id"),
//        col("name").cast("String").as("location_name"),
//        col("distance").cast("Float"),
//        col("distance_unit").cast("String"),
//        col("duration").cast("Int"),
//        col("category").cast("String")
//      )
//
//    hotel_mapping_location_distance.createCassandraTable("testkeyspace", "hotel_location")
//    hotel_mapping_location_distance
//      .write
//      .format("org.apache.spark.sql.cassandra")
//      .mode("Append")
//      .options(Map("table" -> "hotel_location", "keyspace" -> "testkeyspace"))
//      .save()

    Thread.sleep(120000)

    // Cleaning and Filtering cosine_hotel table
    val cosine_hotel_top = cosine_hotel.filter(col("similar_point") > 0.85
      && col("rank_point") === 1)

    val cosine_hotel_top_clean = cosine_hotel_top.select(
      col("id").cast("String").as("table_id"),
      col("hotel_id").cast("Int").as("id"),
      col("domain_id").cast("Int"),
      col("domain_hotel_id").cast("BigInt"),
      col("cosine_name").cast("Double"),
      col("cosine_address").cast("Double"),
      col("distance").cast("Double"),
      col("similar_point").cast("Double"),
      col("rank_point").cast("Int"))
      .dropDuplicates("domain_id", "domain_hotel_id")

    cosine_hotel_top_clean.createCassandraTable("testkeyspace", "cosine_similar")
    cosine_hotel_top_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "cosine_similar", "keyspace" -> "testkeyspace"))
      .save()

    println(Calendar.getInstance().getTime + ": Data is Saved\n")



    dataProcess.dataMapping()
    dataProcess.dataClustering()
    dataProcess.dataRankScore()
  }
}
