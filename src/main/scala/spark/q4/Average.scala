package spark.q4

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Average extends App {

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
    .add(StructField("business_id", StringType, nullable = false))
    .add(StructField("full_address", StringType, nullable = false))
    .add(StructField("categories", StringType, nullable = false))

  val b = businessRdd.map(i => i.mkString.split("::")).filter(_.length == 3).map(e => Row(e(0), e(1), e(2)))
  val businessDf = sparkSession.createDataFrame(b, businessSchema)

  val reviewSchema = new StructType()
    .add(StructField("review_id", StringType, nullable = true))
    .add(StructField("user_id", StringType, nullable = true))
    .add(StructField("business_id", StringType, nullable = true))
    .add(StructField("stars", DoubleType, nullable = true))

  val reviewRdd = sparkContext.textFile(review)
  val r = reviewRdd.map(i => i.mkString.split("::")).filter(_.length == 4)
    .map(e => Row(e(0), e(1), e(2), e(3).toDouble))

  val newYork =
    businessDf
      .filter(_.getString(1).contains("NY"))
      .map(_.getString(0))
      .collect()

  val ratingDf = sparkSession.createDataFrame(r, reviewSchema)
  val tmp =
    ratingDf
      .filter($"business_id".isin(newYork: _*))
      .groupBy("business_id")
      .agg(
        avg($"stars").as("average")
      )
      .orderBy($"average".desc)
      .take(10)
      .map(e => (e.getAs[String]("business_id"), e.getAs[Double]("average")))

  val top10: Array[(String, String, String)] = businessDf.filter($"business_id".isin(tmp.map(_._1): _*))
    .map(e => (e.getString(0), e.getString(1), e.getString(2)))
    .collect()

  sparkContext.parallelize(tmp.map { e =>
    (e._1, top10.filter(_._1 == e._1).head._2, top10.filter(_._1 == e._1).head._3, e._2)
  })
    .repartition(1)
    .saveAsTextFile(output)
}
