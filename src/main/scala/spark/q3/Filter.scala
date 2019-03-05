package spark.q3

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object Filter extends App {

  if (args.length == 0) {
    print("Need Arguments")
    System.exit(1)
  }

  private val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("filter")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext

  import sparkSession.implicits._

  val business = args(0)
  val review = args(1)
  val output = args(2)
  val businessRdd = sparkContext.textFile(business)

  val businessSchema = new StructType()
    .add(StructField("business_id", StringType, nullable = true))
    .add(StructField("full_address", StringType, nullable = true))
    .add(StructField("categories", StringType, nullable = true))

  val b = businessRdd.map(i => i.mkString.split("::")).filter(_.length == 3).map(e => Row(e(0), e(1), e(2)))

  val businessDf = sparkSession.createDataFrame(b, businessSchema)
  val bId: Seq[String] = businessDf.filter("categories like '%Colleges & Universities%'")
    .select("business_id")
    .collect()
    .map(_.mkString)
    .toSeq

  val reviewSchema = new StructType()
    .add(StructField("review_id", StringType, nullable = true))
    .add(StructField("user_id", StringType, nullable = true))
    .add(StructField("business_id", StringType, nullable = true))
    .add(StructField("stars", DoubleType, nullable = true))

  val reviewRdd = sparkContext.textFile(review)
  val r = reviewRdd.map(i => i.mkString.split("::")).filter(_.length == 4)
    .map(e => Row(e(0), e(1), e(2), e(3).toDouble))

  val ratingDf = sparkSession.createDataFrame(r, reviewSchema)
  ratingDf
    .filter($"business_id".isin(bId: _*))
    .map(e => e.getString(0) + "," + e.getDouble(3))
    .repartition(1)
    .rdd
    .saveAsTextFile(output)

}
