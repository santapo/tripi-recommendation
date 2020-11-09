package recommender.engine.core

import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{asc, col, desc}
import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils.{ stripAccents}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scala.util.matching.Regex

class dataReader{

}

object dataReader {

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
                   provider: String,
                   name: String,
                   address: String,
                   price: String,
                   rank: Float,
                   review: Int,
                   star_number: Int,
                   overall_score: Float,
                   checkin_time: String,
                   checkout_time: String,
                   suggest: Array[Map[String,String]])

  def getData(x: Option[Any]) = x match {
    case Some(s) => {
      if (s == null){
        "0"
      }else {
        s.toString
      }
    }
    case None => "0"
    case _ => "0"
  }

  class readData(val hotel_table: Dataset[Row], val suggest: Dataset[Row]){

    def search (page:Int,key:String) : readData = {
      val patternD = new Regex("đ|ð")
      var newkey = patternD.replaceAllIn(key, "d")
      newkey = stripAccents(newkey)

      newkey = newkey.toLowerCase().trim
      val dataHotel = this.hotel_table
        .filter(col("province") === (newkey))
      val dataHotelRank = dataHotel.orderBy(desc("rank"))
      val offset = dataHotelRank.limit(page*5)
      val data = dataHotelRank.except(offset).limit(5).join(this.suggest,Seq("id"))
      val readdata = new readData(data,this.suggest)

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
      .filter(col("price").cast("String")=!=("0"))

    val dataHotel = hotel_table.select(
      col("id"),
      col("provider"),
      col("name"),
      col("province"),
      col("rank"),
      col("address"),
      col("star_number"),
      col("overall_score"),
      col("checkin_time"),
      col("checkout_time"),
      col("price")
    )

    val suggest = hotel_table.select(col("id")
    ,col("suggest"))

    val readdata = new readData(dataHotel,suggest)
    readdata
  }

}
