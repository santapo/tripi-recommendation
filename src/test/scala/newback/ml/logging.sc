import org.apache.spark.sql.functions._

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("tripi/5f1-Data Preprocessing")
  .getOrCreate()

import org.apache.spark.sql.functions.col
import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")


val hotel_logging = spark.read
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_logging")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()

val hotel_logging_clean = hotel_logging.select(
  col("id").cast("String").as("logging_id"),
  col("user_id").cast("BigInt"),
  col("session_id").cast("BigInt"),
  col("action_name").cast("String"),
  col("hotel_id").cast("Int").as("id"),
  col("rank_on_page").cast("Int"),
  col("reviews_number").cast("Int"),
  col("star_number").cast("Int"),
  col("rating_level").cast("Int"),
  col("overall_score").cast("Float"),
  col("price").cast("Int")
)

val mapping_hotel_logging_clean = hotel_logging_clean.select(
  col("logging_id"),
  col("user_id"),
  col("session_id"),
  col("action_name"),
  col("id"),
  col("rank_on_page"),
  col("reviews_number"),
  col("star_number"),
  col("rating_level"),
  col("overall_score"),
  col("price")
)

//val a = mapping_hotel_logging_clean.groupBy().count().take(1)
//
//a(0)(0)
//
//a(0)(0).toString().toInt
//
//a(0)(0).toString().toInt<19999
//
//val map = mapping_hotel_logging_clean.withColumn("overall_score",col("overall_score")*2)
//
//map.show()


val logging_drop_1 = mapping_hotel_logging_clean.filter(col("id")=!="-1" && col("action_name") =!="2_click_detail")

val logging_rank_on_page = logging_drop_1.na.fill(29,Array("rank_on_page"))

val logging_star_number = logging_rank_on_page.na.fill(3,Array("star_number"))

val logging_rating_level = logging_star_number.na.fill(3,Array("rating_level"))

val logging_review_number_1 = logging_rating_level.na.fill(120,Array("reviews_number"))

val logging_review_number_2 = logging_review_number_1
  .withColumn("reviews_number",
    when(col("reviews_number").equalTo("-1"),120).otherwise(col("reviews_number")))

val logging_overall_score = logging_review_number_2.na.fill(80,Array("overall_score"))

val logging_price = logging_overall_score.na.fill(3000000,Array("price"))

val logging_drop_2 = logging_price.filter(col("rank_on_page")<31)

