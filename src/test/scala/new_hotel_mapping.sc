val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix-tripi-dataprocessing")
  .getOrCreate()

import org.apache.spark.sql.functions._
import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

val hotel_mapping = spark.read
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_mapping")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()

val hotel_price_daily = spark.read
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_price_daily")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()

val hotel_price_daily_clean = hotel_price_daily.select(
  col("domain_id").cast("Int"),
  col("domain_hotel_id").cast("BigInt"),
  col("final_amount_min").cast("Float"))

val hotel_mapping_clean = hotel_mapping.select(
  col("id").cast("String"),
  col("domain_id").cast("Int"),
  col("domain_hotel_id").cast("BigInt"),
  col("name").cast("String"),
  col("url").cast("String"),
  col("longitude").cast("Float"),
  col("latitude").cast("Float"),
  col("address").cast("String"),
  col("star_number").cast("Int"),
  col("overall_score").cast("Float"),
  col("checkin_time").cast("String"),
  col("checkout_time").cast("String"))
  .filter(col("url") =!= "")
  .filter(col("address") isNotNull)
  .dropDuplicates("domain_id", "domain_hotel_id")

val price_min_max = hotel_price_daily_clean
  .groupBy("domain_id","domain_hotel_id")
  .agg(max("final_amount_min").as("max_price"),
    min("final_amount_min").as("min_price"),
    avg("final_amount_min").as("avg_price"))

val hotel_mapping_with_price = hotel_mapping_clean
  .join(price_min_max,Seq("domain_id","domain_hotel_id"),"inner")

hotel_mapping_with_price.printSchema()
