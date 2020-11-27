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
                   review_list: Array[Map[String,String]],
                   image_list: Array[String],
                   final_score: Double)

  class readData(val hotel_table:Dataset[Row]){

    def search (page:Int,key:String) : readData = {
//      val patternD = new Regex("đ|ð")
//      var newkey = patternD.replaceAllIn(key, "d")
//      newkey = stripAccents(newkey)
//
//      newkey = newkey.toLowerCase().trim
      val hotel_data = this.hotel_table
        .filter(col("address").contains(key))

      val count_cluster = hotel_data.groupBy("hotel_cluster").count()

      val get_cluster = count_cluster.orderBy(desc("count")).limit(1)

      val hotel_in_cluster = this.hotel_table
        .join(get_cluster,Seq("hotel_cluster"),"inner")
      val hotel_rank = hotel_in_cluster.orderBy(col("final_score").desc)
      val data = hotel_rank.limit(page*5)

      val get_hotel_id = data.select(
        col("id"),
        col("cluster_id")
      )

      val mapping_domain_hotel = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "mapping_domain_hotel", "keyspace" -> "testkeyspace"))
        .load()

      val mapping_id = get_hotel_id
        .join(mapping_domain_hotel,Seq("id"),"inner")

      //
      // Add review_list and image_list
      //

      val hotel_review_with_text = spark.read
        .format("jdbc")
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
        .option("dbtable", "hotel_review")
        .option("user", "FiveF1")
        .option("password", "z3hE3TkjFzNyXhjb6iek")
        .load()

      val hotel_review_text = hotel_review_with_text.select(
        col("id").cast("String").as("table_review_id"),
        col("review_id").cast("Int"),
        col("domain_id").cast("Int"),
        col("domain_hotel_id").cast("BigInt"),
        col("username").cast("String"),
        col("text").cast("String"),
        col("review_datetime").cast("Date"),
        col("score").cast("Float")
      )

      val review_data = hotel_review_text
        .join(mapping_id,Seq("domain_id","domain_hotel_id"),"inner")

      def limitSize(n: Int, arrCol: Column): Column =
        array((0 until n).map(arrCol.getItem):_*)

      val mapping_review_count_word = review_data
        .withColumn("word_count", size(split(col("text")," ")))
        .filter(col("word_count")>20)

      val review_list = mapping_review_count_word
        .withColumn("review_list",
          mapReviewUdf(col("username"),col("domain_id"),col("text"),col("score"),col("review_datetime")))

      val review_list_clean = review_list
        .groupBy("id").agg(
        collect_list(col("review_list")).as("review_list")
      ).select(
        col("id"),
        limitSize(3,col("review_list")).as("review_list")
      )

      val hotel_image = spark.read
        .format("jdbc")
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
        .option("dbtable", "hotel_image")
        .option("user", "FiveF1")
        .option("password", "z3hE3TkjFzNyXhjb6iek")
        .load()

      val mapping_image = hotel_image
        .join(mapping_id,Seq("domain_id","domain_hotel_id"),"inner")

      val mapping_image_list = mapping_image
        .groupBy("id").agg(
        collect_list(col("provider_url")).as("image_list")
      ).select(
        col("id"),
        limitSize(10,col("image_list")).as("image_list")
      )

      val data_final = data
        .join(mapping_image_list,Seq("id"),"left")
        .join(review_list_clean,Seq("id"),"left")

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
        col("review_list"),
        col("image_list"),
        col("final_score")
      )

      val readdata = new readData(data_final_clean)

      readdata
    }

    def load(): String = {
      val schema = Encoders.product[hotel]
      org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

      val data = this.hotel_table
        .na.drop()
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
