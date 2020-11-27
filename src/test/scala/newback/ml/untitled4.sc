import org.apache.spark.sql.functions.array

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix-tripi-dataprocessing")
  .getOrCreate()

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

val hotel_review = spark.read
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_review")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()

val hotel_review_clean = hotel_review.select(
  col("id").cast("String").as("table_review_id"),
  col("review_id").cast("Int"),
  col("domain_id").cast("Int"),
  col("domain_hotel_id").cast("BigInt"),
  col("username").cast("String"),
  col("review_datetime").cast("Date"),
  col("score").cast("Float")
)

val connector = CassandraConnector(sparkContext.getConf)

import com.datastax.spark.connector._

def limitSize(n: Int, arrCol: Column): Column =
  array((0 until n).map(arrCol.getItem):_*)

hotel_review_clean.createCassandraTable("testkeyspace","hotel_review_3")
hotel_review_clean
  .write
  .format("org.apache.spark.sql.cassandra")
  .mode("Append")
  .options(Map("table" -> "hotel_review", "keyspace" -> "testkeyspace"))
  .save()