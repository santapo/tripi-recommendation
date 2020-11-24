import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

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

val mapping_root = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mapping_root", "keyspace" -> "testkeyspace"))
  .load()


//mapping_root.printSchema()
//
//mapping_root.show()
//
//mapping_root.groupBy().count().show()
//mapping_root.show
//case class dataset(id: String,
//                   longitude: Float,
//                   latitude: Float)
//

val geolocation = mapping_root.select(
  col("id"),
  col("longitude"),
  col("latitude")
)
//
//val schema = Encoders.product[dataset]
//val data = geolocation.as[dataset](schema)
//
//
val cols = Array("latitude","longitude")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featureDf = assembler.transform(geolocation)

val featureDf2 = featureDf.select(
  col("id"),
  col("longitude"),
  col("latitude"),
  col("features")).toDF()


//
//featureDf.show()
//
//featureDf.show(false)
//featureDf.printSchema()
//
//
val kmeans = new KMeans()
  .setK(100)
  .setFeaturesCol("features")
  .setPredictionCol("prediction")

val kmeansModel = kmeans.fit(featureDf2)

val result = kmeansModel.transform(featureDf2)

result.show()

result.groupBy().count().show()


result.groupBy("prediction").count().show(100)

val cluster_id = result.select(
  col("id").cast("String"),
  col("prediction").cast("Int")
)

val table = mapping_root.join(cluster_id,Seq("id"),"inner")

table.show()