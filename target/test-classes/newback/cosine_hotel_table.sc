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

val cosine_similar = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "cosine_similar", "keyspace" -> "testkeyspace"))
  .load()

cosine_similar.show()

cosine_similar.printSchema()

cosine_similar.groupBy().count().show()


