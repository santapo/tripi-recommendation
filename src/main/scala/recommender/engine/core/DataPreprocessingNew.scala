package recommender.engine.core

import java.util.Calendar

import akka.actor.Actor
import Udf._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._

class DataPreprocessingNew {
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("phoenix-tripi-dataprocessing")
    .getOrCreate()

  import spark.implicits._

  val sparkContext = spark.sparkContext

  sparkContext.setLogLevel("WARN")

  val connector = CassandraConnector(sparkContext.getConf)
  connector.withSessionDo(session => {
    session.execute("DROP KEYSPACE IF EXISTS testkeyspace")
    session.execute("CREATE KEYSPACE testkeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute("USE testkeyspace")
    session.execute("CREATE TABLE testkeyspace.hotel_table " +
      "(id text PRIMARY KEY," +
      " provider int," +
      " province text," +
      " name text," +
      " rank double," +
      " address text," +
      " star_number int," +
      " overall_score float," +
      " price text," +
      " suggest list<frozen <map<text,text>>>)")
  }
  )

  val dataMap = new DataProcessing

  def dataToCassandra(): Unit = {

    /**
     * Read data from Clickhouse Database then save in Cassandra
     **/

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

    val hotel_info = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_info")
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

    val hotel_price_daily = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "hotel_price_daily")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val domain = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "domain")
      .option("user", "FiveF1")
      .option("password", "z3hE3TkjFzNyXhjb6iek")
      .load()

    val province = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
      .option("dbtable", "province")
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
      .agg(max("final_amount_min").as("max_price"),
        min("final_amount_min").as("min_price"),
        avg("final_amount_min").as("avg_price"))

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
      col("longitude").cast("Float").as("long_mapping"),
      col("latitude").cast("Float").as("lat_mapping"),
      col("star_number").cast("Int").as("star_mapping"),
      col("overall_score").cast("Float").as("overall_mapping"),
      col("checkin_time").cast("String").as("checkin_mapping"),
      col("checkout_time").cast("String").as("checkout_mapping"),
      col("max_price").cast("Float"),
      col("min_price").cast("Float"),
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
      col("province_id").cast("Int"),
      col("district_id").cast("Int"),
      col("street_id").cast("Int"),
      col("star_number").cast("Int"),
      col("checkin_time").cast("String"),
      col("checkout_time").cast("String"),
      col("overall_score").cast("Float"))

    roothotel_info_clean.createCassandraTable("testkeyspace", "root_hotel")
    roothotel_info_clean
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "root_hotel", "keyspace" -> "testkeyspace"))
      .save()

    Thread.sleep(200000)

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

    dataMapping()
  }

  def dataMapping(): Unit = {
    println(Calendar.getInstance().getTime + ": Start data mapping...\n")

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

    hotel_mapping.printSchema()
    hotel_mapping.groupBy().count().show()

    cosine_similar.printSchema()
    cosine_similar.groupBy().count().show()

    root_hotel.printSchema()
    root_hotel.groupBy().count().show()

    val mapping_root = cosine_similar
      .join(hotel_mapping,Seq("domain_id","domain_hotel_id"),"inner")
      .join(root_hotel,Seq("id"),"inner")

    mapping_root.createCassandraTable("testkeyspace", "mapping_root")
    mapping_root
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "mapping_root", "keyspace" -> "testkeyspace"))
      .save()
  }
}



case object dataToCassandra

class dataPreprocessingNewActor(DataPreprocessingNew: DataPreprocessingNew) extends Actor{

  // Implement receive mehtod
  def  receive = {
    case dataFiltering => {
      println(Calendar.getInstance().getTime + ": Data is Saving to Cassandra... \n")
      DataPreprocessingNew.dataToCassandra()
    }
  }
}
