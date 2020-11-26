package recommender.engine.core

import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{asc, col, desc}
import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils.{ stripAccents}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scala.util.matching.Regex

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
                   image_list: Array[String])

  class readData(val hotel_table:Dataset[Row]){

    def search (page:Int,key:String) : readData = {
//      val patternD = new Regex("đ|ð")
//      var newkey = patternD.replaceAllIn(key, "d")
//      newkey = stripAccents(newkey)
//
//      newkey = newkey.toLowerCase().trim
      val hotel_data = this.hotel_table
        .filter(col("address").contains(key))
      val hotel_rank = hotel_data.orderBy(col("rank").desc)
      val data = hotel_rank.limit(page*5)
      val readdata = new readData(data)

      readdata
    }

    def load(): String = {



      val data = this.hotel_table

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
