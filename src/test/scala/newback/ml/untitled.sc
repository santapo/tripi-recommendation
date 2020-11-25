import org.apache.spark.sql.functions.{col, current_date, datediff, lit, to_date, udf}

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
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_location")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()

val hotel_distance_to_location = spark.read
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_distance_to_location")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()



val hotel_location_clean = hotel_location.select(
  col("id").cast("String").as("location_id"),
  col("domain_id").cast("Int"),
  col("hotel_id").cast("String")
)

val hotel_mapping_location_distance = hotel_distance_to_location
  .join(hotel_location_clean,Seq("location_id","domain_id"),"inner")
  .select(
    col("id").cast("String"),
    col("domain_id"),
    col("hotel_id"),
    col("name").cast("String").as("location_name"),
    col("distance").cast("Float"),
    col("distance_unit").cast("String"),
    col("duration").cast("Int"),
    col("category").cast("String")
  )

hotel_mapping_location_distance.printSchema()

hotel_mapping_location_distance.show()

hotel_mapping_location_distance.groupBy().count().show()