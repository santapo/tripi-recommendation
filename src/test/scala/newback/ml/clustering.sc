import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.col

val spark = org.apache.spark.sql.SparkSession
  .builder()
  .config("spark.debug.maxToStringFields", 100)
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


mapping_root.printSchema()
//
//mapping_root.show()
//
//mapping_root.groupBy().count().show()

val geolocation = mapping_root.select(
  col("id"),
  col("longitude"),
  col("latitude")
)

val cols = Array("latitude","longitude")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featureDf = assembler.transform(geolocation).select("id","features")

val df = featureDf.na.drop()

val kmeans = new KMeans()
  .setK(3)
  .setSeed(1)
  .setFeaturesCol("features")

geolocation.show(10000)
