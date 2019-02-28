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

  val business = args(0)
  val review = args(1)
  val output = args(2)
  val bDf = sparkSession.read.format("csv").load(business)

  val businessSchema = new StructType()
    .add(StructField("business_id", StringType, nullable = true))
    .add(StructField("full_address", StringType, nullable = true))
    .add(StructField("categories", StringType, nullable = true))

  val b = bDf.rdd.map(i => i.mkString.split("::")).filter(_.length == 3).map(e => Row(e(0), e(1), e(2)))

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

  val rDf = sparkSession.read.format("csv").load(review)
  val r = rDf.rdd.map(i => i.mkString.split("::")).filter(_.length == 4)
    .map(e => Row(e(0), e(1), e(2), e(3).toDouble))

  import sparkSession.implicits._

  val ratingDf = sparkSession.createDataFrame(r, reviewSchema)
  ratingDf
    .filter($"business_id".isin(bId: _*))
    .map(e => e.getString(0) + "," + e.getDouble(3))
    .repartition(1)
    .write
    .text(output)

}
