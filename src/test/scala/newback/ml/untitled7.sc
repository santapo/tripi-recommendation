val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("tripi/5f1-Data Preprocessing")
  .getOrCreate()

import io.netty.util.internal.EmptyArrays
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

val hotel_image = spark.read
  .format("jdbc")
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("url", "jdbc:clickhouse://phoenix-db.data.tripi.vn:443/PhoeniX?ssl=true&charset=utf8")
  .option("dbtable", "hotel_image")
  .option("user", "FiveF1")
  .option("password", "z3hE3TkjFzNyXhjb6iek")
  .load()

val mapping_domain_hotel = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mapping_domain_hotel", "keyspace" -> "testkeyspace2"))
  .load()

val mapping_image = hotel_image
  .join(mapping_domain_hotel,Seq("domain_id","domain_hotel_id"),"inner")


def limitSize(n: Int, arrCol: Column): Column =
  array((0 until n).map(arrCol.getItem):_*)

val mapping_image_list = mapping_image
  .groupBy("id").agg(
  collect_list(col("provider_url")).as("image_list")
).select(
  col("id"),
  limitSize(10,col("image_list")).as("image_list")
)

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._


val connector = CassandraConnector(sparkContext.getConf)
connector.withSessionDo(session => {
  session.execute("USE testkeyspace2")
  session.execute("CREATE TABLE testkeyspace2.mapping_image_list_5 " +
                  "(id int PRIMARY KEY," +
                  "image_list list<text>)")
})

//mapping_image_list.show()

mapping_image_list.filter(size(col("image_list"))>0)

val mapping_image_list_clean = mapping_image_list.filter(size(col("image_list"))>0)

//mapping_image_list_clean.show()




mapping_image_list_clean
  .write
  .format("org.apache.spark.sql.cassandra")
  .mode("Append")
  .options(Map("table" -> "mapping_image_list_5", "keyspace" -> "testkeyspace2"))
  .save()



