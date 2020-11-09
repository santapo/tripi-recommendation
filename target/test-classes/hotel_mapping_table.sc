val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
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

hotel_mapping.show()

hotel_mapping.printSchema()

hotel_mapping.groupBy().count().show()



