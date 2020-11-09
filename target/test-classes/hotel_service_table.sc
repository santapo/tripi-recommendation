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

val hotel_service = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "hotel_service", "keyspace" -> "testkeyspace"))
  .load()

hotel_service.show()

hotel_service.printSchema()

hotel_service.groupBy().count().show()


