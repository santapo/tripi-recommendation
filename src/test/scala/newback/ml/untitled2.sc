val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix-tripi-dataprocessing")
  .getOrCreate()

val sparkContext = spark.sparkContext

import spark.implicits._

sparkContext.setLogLevel("WARN")

val hotel_location = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_location", "keyspace" -> "testkeyspace"))
  .load()

hotel_location.show()

hotel_location.groupBy("category").count().show()

hotel_location.groupBy("distance_unit").count().show()

