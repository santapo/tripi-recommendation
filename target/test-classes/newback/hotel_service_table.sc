import org.apache.spark.sql.functions.{col, max}

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix-tripi-dataprocessing")
  .getOrCreate()

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

val hotel_service = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_service", "keyspace" -> "testkeyspace"))
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

mapping_root.printSchema()

hotel_service.printSchema()

mapping_domain_hotel.printSchema()

val mapping_root_service = mapping_domain_hotel
  .join(hotel_service,Seq("hotel_id"),"inner")

mapping_root_service.groupBy().count().show()
mapping_root_service.printSchema()
val mapping_root_service_agg = mapping_root_service
  .groupBy("id").agg(
  max(col("airport_transfer")),
  max(col("baby_sitting"))
  )

mapping_root_service_agg.show()

mapping_root_service_agg.groupBy().count()


mapping_root_service_agg.groupBy().count().show()

mapping_root_service_agg.groupBy("max(airport_transfer)").count().show()

mapping_domain_hotel.groupBy("id").count().groupBy().count().show()

