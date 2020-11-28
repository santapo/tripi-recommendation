package recommender.engine.core

import org.apache.spark.sql.{Column, Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{array, asc, col, collect_list, desc, size, split}

import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils.stripAccents
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.util.matching.Regex
import recommender.engine.core.Udf.mapReviewUdf

class DataReader{

}

object DataReader {

  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields",100)
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("tripi/5f1 - Batch Processing")
    .getOrCreate()

  import spark.implicits._

  val sparkContext = spark.sparkContext

  sparkContext.setLogLevel("WARN")

  case class hotel(id: String,
                   name: String,
                   address: String,
                   logo: String,
                   star_number: Int,
                   checkin_time: String,
                   checkout_time: String,
                   overall_score: Float,
                   description: String,
                   avg_price: Float,
                   longitude: Float,
                   latitude: Float,
                   review_count: Int,
                   suggest: Array[Map[String,String]],
                   image_list: Array[String],
                   final_score: Double,
                   cleanliness_score: Float,
                   meal_score: Float,
                   location_score: Float,
                   service_score: Float,
                   sleep_quality_score: Float,
                   tours: Int,
                   night_club: Int,
                   relax_spa: Int,
                   relax_sauna: Int,
                   room_service_24_hour: Int,
                   poolside_bar: Int,
                   restaurants: Int,
                   shops: Int,
                   bar: Int
                  )

  case class province(province_name: String
                      //count: Int
                  )

  class readData(val hotel_table:Dataset[Row]){

    def search (page:Int,key:String) : readData = {
      val patternD = new Regex("đ|ð")
      var newkey = patternD.replaceAllIn(key, "d")
      newkey = stripAccents(newkey)

      newkey = newkey.toLowerCase().trim

      val hotel_data_1 = this.hotel_table
        .filter(col("province_name").contains(newkey))

      val hotel_data_2 = this.hotel_table
        .filter(col("district_name").contains(newkey))

      val hotel_data_3 = this.hotel_table
        .filter(col("street_name").contains(newkey))

      val hotel_data = hotel_data_1.union(hotel_data_2).union(hotel_data_3)

      val hotel_rank = hotel_data.orderBy(col("final_score").desc)
      val data = hotel_rank.limit(page*5)

      val get_hotel_id = data.select(
        col("id"),
        col("hotel_cluster")
      )

      val mapping_domain_hotel = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "mapping_domain_hotel", "keyspace" -> "testkeyspace"))
        .load()

      val mapping_id = get_hotel_id
        .join(mapping_domain_hotel,Seq("id"),"inner")

      def limitSize(n: Int, arrCol: Column): Column =
        array((0 until n).map(arrCol.getItem):_*)

      val mapping_image_1 = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "mapping_image", "keyspace" -> "testkeyspace"))
        .load()

      val mapping_image = mapping_image_1
        .join(mapping_id,Seq("domain_id","domain_hotel_id","id"),"inner")

      val mapping_image_list = mapping_image
        .groupBy("id").agg(
        collect_list(col("provider_url")).as("image_list")
      ).select(
        col("id"),
        limitSize(10,col("image_list")).as("image_list")
      )

      val mapping_service = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "mapping_service", "keyspace" -> "testkeyspace"))
        .load()

      val data_final = data
        .join(mapping_image_list,Seq("id"),"left")
        .join(mapping_service,Seq("id"),"left")

      val data_final_clean = data_final.select(
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
        col("image_list"),
        col("final_score"),
        col("cleanliness_score"),
        col("meal_score"),
        col("location_score"),
        col("service_score"),
        col("sleep_quality_score"),
        col("tours"),
        col("night_club"),
        col("relax_spa"),
        col("relax_sauna"),
        col("room_service_24_hour"),
        col("poolside_bar"),
        col("restaurants"),
        col("shops"),
        col("bar")
      )

      val readdata = new readData(data_final_clean)

      readdata

      //
      // Add review_list and image_list
      //

//      val mapping_review_text = spark.read
//        .format("jdbc")
//        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//        .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
//        .option("dbtable", "hotel_review")
//        .option("user", "FiveF1")
//        .option("password", "z3hE3TkjFzNyXhjb6iek")
//        .load()
//
//      val hotel_review_text = hotel_review_with_text.select(
//        col("id").cast("String").as("table_review_id"),
//        col("review_id").cast("Int"),
//        col("domain_id").cast("Int"),
//        col("domain_hotel_id").cast("BigInt"),
//        col("username").cast("String"),
//        col("text").cast("String"),
//        col("review_datetime").cast("Date"),
//        col("score").cast("Float")
//      )

//      val mapping_review_text = spark.read
//        .format("org.apache.spark.sql.cassandra")
//        .options(Map("table" -> "mapping_review_text", "keyspace" -> "testkeyspace"))
//        .load()
//
//      val review_data = mapping_review_text
//        .join(mapping_id,Seq("domain_id","domain_hotel_id"),"inner")
//

//
//      val mapping_review_count_word = review_data
//        .withColumn("word_count", size(split(col("text")," ")))
//        .filter(col("word_count")>20)
//
//      val review_list = mapping_review_count_word
//        .withColumn("review_list",
//          mapReviewUdf(col("username"),col("domain_id"),col("text"),col("score"),col("review_datetime")))
//
//      val review_list_clean = review_list
//        .groupBy("id").agg(
//        collect_list(col("review_list")).as("review_list")
//      ).select(
//        col("id"),
//        limitSize(3,col("review_list")).as("review_list")
//      )

//      val hotel_image = spark.read
//        .format("jdbc")
//        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//        .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
//        .option("dbtable", "hotel_image")
//        .option("user", "FiveF1")
//        .option("password", "z3hE3TkjFzNyXhjb6iek")
//        .load()



//      val mapping_image_list = spark.read
//        .format("org.apache.spark.sql.cassandra")
//        .options(Map("table" -> "mapping_image_list", "keyspace" -> "testkeyspace"))
//        .load()
//
//      val mapping_review_list = spark.read
//        .format("org.apache.spark.sql.cassandra")
//        .options(Map("table" -> "mapping_review_list", "keyspace" -> "testkeyspace"))
//        .load()

    }

    def lowest_price (page:Int,key:String) : readData = {
      val patternD = new Regex("đ|ð")
      var newkey = patternD.replaceAllIn(key, "d")
      newkey = stripAccents(newkey)

      newkey = newkey.toLowerCase().trim

      val hotel_data_1 = this.hotel_table
        .filter(col("province_name").contains(newkey))

      val hotel_data_2 = this.hotel_table
        .filter(col("district_name").contains(newkey))

      val hotel_data_3 = this.hotel_table
        .filter(col("street_name").contains(newkey))

      val hotel_data = hotel_data_1.union(hotel_data_2).union(hotel_data_3)

      val hotel_rank = hotel_data.orderBy(col("avg_price").asc)
      val data = hotel_rank.limit(page * 5)

      val get_hotel_id = data.select(
        col("id"),
        col("hotel_cluster")
      )

      val mapping_domain_hotel = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "mapping_domain_hotel", "keyspace" -> "testkeyspace"))
        .load()

      val mapping_id = get_hotel_id
        .join(mapping_domain_hotel, Seq("id"), "inner")

      def limitSize(n: Int, arrCol: Column): Column =
        array((0 until n).map(arrCol.getItem): _*)

      val mapping_image_1 = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "mapping_image", "keyspace" -> "testkeyspace"))
        .load()

      val mapping_image = mapping_image_1
        .join(mapping_id, Seq("domain_id", "domain_hotel_id", "id"), "inner")

      val mapping_image_list = mapping_image
        .groupBy("id").agg(
        collect_list(col("provider_url")).as("image_list")
      ).select(
        col("id"),
        limitSize(10, col("image_list")).as("image_list")
      )

      val mapping_service = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "mapping_service", "keyspace" -> "testkeyspace"))
        .load()

      val data_final = data
        .join(mapping_image_list, Seq("id"), "left")
        .join(mapping_service, Seq("id"), "left")

      val data_final_clean = data_final.select(
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
        col("image_list"),
        col("final_score"),
        col("cleanliness_score"),
        col("meal_score"),
        col("location_score"),
        col("service_score"),
        col("sleep_quality_score"),
        col("tours"),
        col("night_club"),
        col("relax_spa"),
        col("relax_sauna"),
        col("room_service_24_hour"),
        col("poolside_bar"),
        col("restaurants"),
        col("shops"),
        col("bar")
      )

      val readdata = new readData(data_final_clean)

      readdata
    }

    def top_province (page:Int) : readData = {
      val dataHotel = this.hotel_table
      //  .groupBy("province_name").avg("final_score")
      //val dataHotelRank = dataHotel.orderBy(col("avg(final_score)").desc).withColumnRenamed("avg(final_score)","avg_score")
      val hotel_logging = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "hotel_logging", "keyspace" -> "testkeyspace"))
        .load()
      dataHotel.join(hotel_logging, dataHotel("id") === hotel_logging("id"))
      val province_rank = dataHotel.groupBy("province_name").count().orderBy(col("count").desc)
      println(province_rank)
      val data = province_rank.limit(page*5)
      val readdata = new readData(data)
      readdata
    }

    def load_province(): String = {
      val schema = Encoders.product[province]
      org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
      val data = this.hotel_table
        .as[province](schema)
        .collectAsList
        .toList
      val jsonString = Json(DefaultFormats).write(data)

      this.hotel_table.unpersist()
      jsonString
    }

    def load(): String = {
      val schema = Encoders.product[hotel]
      org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

      val data = this.hotel_table
        .as[hotel](schema)
        .collectAsList
        .toList
      val jsonString = Json(DefaultFormats).write(data)

      this.hotel_table.unpersist()
      jsonString
    }
  }

  def readData (): readData = {

    val hotel_table = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "hotel_table", "keyspace" -> "testkeyspace"))
      .load()
      .cache()

    val readdata = new readData(hotel_table)
    readdata
  }

}
