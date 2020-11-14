import org.apache.spark.sql.functions.col

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields",100)
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

hotel_mapping.printSchema()
hotel_mapping.groupBy().count().show()

cosine_similar.printSchema()
cosine_similar.groupBy().count().show()

root_hotel.printSchema()
root_hotel.groupBy().count().show()

val mapping_root = cosine_similar
  .join(hotel_mapping,Seq("domain_id","domain_hotel_id"),"inner")
  .join(root_hotel,Seq("id"),"inner")

mapping_root.printSchema()

mapping_root.show()

mapping_root.show()

mapping_root.groupBy().count().show()

mapping_root.groupBy("domain_id").count().show()

mapping_root.groupBy("domain_hotel_id").count().show()

mapping_root.groupBy("id").count().orderBy(col("count").desc).show()


mapping_root.groupBy("domain_hotel_id").count().orderBy(col("count").desc).show()

mapping_root.where(col("domain_hotel_id") === 256084).show()

mapping_root.groupBy("hotel_id").count().orderBy(col("count").desc).show()