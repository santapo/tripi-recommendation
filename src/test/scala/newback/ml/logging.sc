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

mapping_hotel_logging_clean.show()

mapping_hotel_logging_clean.groupBy().count().show()

val logging_drop_1 = mapping_hotel_logging_clean.filter(col("hotel_id")==="-1")

logging_drop_1.groupBy().count().show()

val logging_drop_2 = mapping_hotel_logging_clean.filter(col("hotel_id")=!="-1")

logging_drop_2.groupBy("rating_level").count().show()

logging_drop_2.groupBy("rank_on_page").count().show()

logging_drop_2.describe("rank_on_page").show()

val logging_rank_on_page = logging_drop_2.na.fill(29,Array("rank_on_page"))

logging_rank_on_page.describe("rank_on_page").show()

logging_drop_2.describe("rating_level").show()

import org.apache.spark.sql.functions._

logging_drop_2.groupBy("id").agg(
  max(col("star_number")).as("star_number")
).describe("star_number")

logging_drop_2.groupBy("id").agg(
  max(col("star_number")).as("star_number")
).describe("star_number").show()

logging_drop_2.describe("star_number").show()

val logging_star_number = logging_rank_on_page.na.fill(3,Array("star_number"))

logging_star_number.describe("star_number").show()

logging_star_number.groupBy("id").agg(
  max(col("rating_level")).as("rating_level")
).describe("rating_level").show()

val logging_rating_level = logging_star_number.na.fill(3,Array("rating_level"))

logging_star_number.groupBy("id").agg(
  max(col("reviews_number")).as("reviews_number")
).describe("reviews_number").show()

val logging_review_number_1 = logging_rating_level.na.fill(120,Array("reviews_number"))

val logging_review_number_2 = logging_review_number_1
  .where(col("reviews_number")==="-1").show()

val logging_review_number_3 = logging_review_number_1
  .withColumn("reviews_number",
    when(col("reviews_number").equalTo("-1"),120).otherwise(col("reviews_number")))

logging_review_number_3.groupBy("id").agg(
  max(col("reviews_number")).as("reviews_number")
).describe("reviews_number").show()

logging_review_number_3.where(col("overall_score")==="-1").show()

logging_review_number_3.describe("overall_score").show()

val logging_overall_score = logging_review_number_3.na.fill(80,Array("overall_score"))

logging_overall_score.where(col("price")==="-1").show()

logging_overall_score.describe("price").show()

logging_overall_score.groupBy("id").agg(
  max(col("price")).as("price")
).describe("price").show()


val logging_price = logging_overall_score.na.fill(3000000,Array("price"))

