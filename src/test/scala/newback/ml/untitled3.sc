import org.apache.spark.sql.functions.{col, lower}
import shapeless.syntax.std.tuple.unitTupleOps

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields",100)
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local[*]")
  .appName("phoenix-tripi-dataprocessing")
  .getOrCreate()

import spark.implicits._

val sparkContext = spark.sparkContext

sparkContext.setLogLevel("WARN")

val mapping_root = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mapping_root", "keyspace" -> "testkeyspace"))
  .load()

mapping_root.show()

mapping_root.withColumn("low_address", lower(col("address"))).show()

val new_root = mapping_root.withColumn("low_address", lower(col("address")))

val data = new_root.
  filter(col("address").like("quan ba"))

data.show()


val data2 = new_root.
  filter(col("address").rlike("quan ba"))

data2.show()


val data3 = new_root.
  filter(col("address").contains("quan ba"))

data3.show()


val data4 = new_root.
  filter(col("address").like("Quan Ba"))

data4.show()


val data5 = new_root.
  filter(col("address").like("quản bạ"))

data5.show()

val data7 = new_root.
  filter(col("address").like("ha noi"))

data7.show()

val data8 = new_root.
  filter(col("address").like("tieu khu"))

data8.show()


val data9 = new_root.
  filter(col("address").like("iTeu Khu"))

data9.show()


val data10 = new_root.
  filter(col("address").like("Tieu Khu"))

data10.show()

val data11 = new_root.filter(col("address").contains("Tieu Khu"))

data11.show()
