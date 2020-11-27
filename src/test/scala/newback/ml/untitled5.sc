val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix-tripi-dataprocessing")
  .getOrCreate()

import org.apache.spark.sql.functions.col
import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

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
val mapping_root = cosine_similar
  .join(hotel_mapping,Seq("domain_id","domain_hotel_id"),"inner")
  .join(root_hotel,Seq("id"),"inner")

val mapping_domain_hotel = mapping_root.select(
  col("table_id"),
  col("id"),
  col("hotel_id"),
  col("domain_id"),
  col("domain_hotel_id")
)
val hotel_review = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_review", "keyspace" -> "testkeyspace"))
  .load()