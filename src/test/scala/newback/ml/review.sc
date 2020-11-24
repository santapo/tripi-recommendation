import breeze.numerics.log
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

val mapping_review = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mapping_review", "keyspace" -> "testkeyspace"))
  .load()

val new_1 = mapping_review.withColumn("adsfsadf",lit(30)*col("score")).show()
//mapping_review.show()
//
//mapping_review.groupBy("id").count().show()
//
//mapping_review.printSchema()
//
//mapping_review.groupBy().count().show()
//
//val new_review = mapping_review
//  .withColumn("end_date",to_date(lit("2020-08-30")))
//  .withColumn("date_distance",datediff(col("end_date"),col("review_datetime")))
//  .filter(col("date_distance") < 800)
//
//new_review.groupBy().count().show()
//
//new_review.describe("date_distance").show()
//
//val review_group_id = new_review.groupBy("id").count()
//
//review_group_id.show()
//
//review_group_id.describe("count").show()
//
//new_review.show()
//
//val calculateReview_1st = (date_distance: Int, Score: Float) =>{
//  val quality_score = Score*1/date_distance
//  quality_score
//}
//
//val calulateReview_1stUdf = udf(calculateReview_1st)
//
//val review_score = new_review
//  .withColumn("quality_score",calulateReview_1stUdf(col("date_distance"),col("score")))
//
//review_score.show()
//
//new_review.groupBy("id").avg().show()
//
//review_group_id.groupBy("id").avg().show()
//

//
//new_review.withColumn("final_review_score", )
// def calculateReview(count)
