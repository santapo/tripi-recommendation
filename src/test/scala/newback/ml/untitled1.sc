
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{avg, col, count, current_date, datediff, first, lit, pow, sum, to_date, udf}
import recommender.engine.core.Udf.{sigmoidPriceUdf, sigmoidServiceUdf}

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

val mapping_review = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mapping_review", "keyspace" -> "testkeyspace"))
  .load()

val mapping_service = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mapping_service", "keyspace" -> "testkeyspace"))
  .load()

val geolocation = mapping_root.select(
  col("id"),
  col("longitude"),
  col("latitude"),
  col("review_count")
)

val cols = Array("latitude","longitude")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featureDf = assembler.transform(geolocation)

val kmeans = new KMeans()
  .setK(80)
  .setFeaturesCol("features")
  .setPredictionCol("hotel_cluster")

val kmeansModel = kmeans.fit(featureDf)

val cluster = kmeansModel.transform(featureDf)

val cluster_clean = cluster.select(
  col("id"),
  col("hotel_cluster"),
  col("review_count")
)

//cluster_clean.show()
//cluster_clean.groupBy().count().show()
val mapping_price_with_cluster = mapping_root.select(
  col("id"),
  col("avg_price")
).join(cluster_clean,Seq("id"),"inner")

val price_quantity_cluster = mapping_price_with_cluster
  .groupBy("hotel_cluster").agg(
  first(col("hotel_cluster")).as("hotel_cluster"),
  avg(col("avg_price")).as("avg_price_cluster")
).withColumn("Quantity",lit(1.44)*col("avg_price_cluster"))

val price_score = mapping_price_with_cluster
  .join(price_quantity_cluster,Seq("hotel_cluster"),"inner")
  .withColumn("price_score",sigmoidPriceUdf(col("avg_price"),col("avg_price_cluster")))

val service_score = mapping_service
  .withColumn("service_score",sigmoidServiceUdf(
    col("relax_spa"),
    col("relax_massage"),
    col("relax_outdoor_pool"),
    col("relax_sauna"),
    col("cleanliness_score"),
    col("meal_score")).cast("Float"))

val service_per_price_score = price_score
  .join(service_score,Seq("id"),"inner")
  .withColumn("service_per_price_score",col("service_score")/col("price_score"))

val service_per_price_score_clean = service_per_price_score.select(
  col("id"),
  col("price_score"),
  col("service_score"),
  col("service_per_price_score")
)

service_per_price_score_clean.describe("service_per_price_score").show()