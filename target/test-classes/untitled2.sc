
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{asc, col, collect_list, desc}

import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils.stripAccents
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.util.matching.Regex

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields",100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("tripi/5f1 - Batch Processing")
  .getOrCreate()

import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

case class hotel(id: String,
                 address: String,
                 name: String,
                 overall_score: Float,
                 price: String,
                 provider: Int,
                 province: String,
                 rank: Double,
                 star_number: Int,
                 suggest: List[Map[String,String]])

val hotel_table = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_table", "keyspace" -> "testkeyspace"))
  .load()
  .cache()
  .filter(col("price").cast("String")=!=("0"))

val dataHotel = hotel_table
  .filter(col("province") === ("nghe an"))
val dataHotelRank = dataHotel.orderBy(col("rank").desc)
val data = dataHotelRank.limit(5)

hotel_table.printSchema()
val schema = Encoders.product[hotel]

val data_1 = data
  .na.drop()
val jsonString = Json(DefaultFormats).write(data_1)

data_1.printSchema()

data_1.toDF()

data.as[hotel](schema)

