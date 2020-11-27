val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields",100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix")
  .getOrCreate()

import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

val hotel_logging = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_logging", "keyspace" -> "testkeyspace"))
  .load()

hotel_logging.groupBy().count().show()

hotel_logging.groupBy("action_name").count().show()

hotel_logging.show()

hotel_logging.printSchema()

hotel_logging.describe("adult_num").show()

import org.apache.spark.sql.functions._

hotel_logging.groupBy("adult_num").count().show()

hotel_logging.groupBy("id").count().show()

hotel_logging.groupBy("rank_on_page").count().show()

hotel_logging.groupBy("session_id").count().groupBy().count().show()

hotel_logging.groupBy("session_id").count().show(1000,false)

hotel_logging.groupBy("session_id").count()
  .orderBy(col("count").asc)
  .show(1000,false)

hotel_logging.where(col("session_id")==="-2479622442145743418").show()