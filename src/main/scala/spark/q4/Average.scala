package spark.q4

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Average extends App {

  if(args.length == 0) {
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

  val reviewSchema = new StructType()
    .add(StructField("review_id", StringType, nullable = true))
    .add(StructField("user_id", StringType, nullable = true))
    .add(StructField("business_id", StringType, nullable = true))
    .add(StructField("stars", DoubleType, nullable = true))

  val rDf = sparkSession.read.format("csv").load(review)
  val r = rDf.rdd.map(i => i.mkString.split("::")).filter(_.length == 4)
    .map(e => Row(e(0), e(1), e(2), e(3).toDouble))

  import sparkSession.implicits._

  val newYork =
    businessDf
      .filter(_.getString(1).contains("NY"))
      .map(_.getString(0))
      .collect()

  val ratingDf = sparkSession.createDataFrame(r, reviewSchema)
  val tmp =
    ratingDf
      .filter($"business_id".isin(newYork:_*))
      .groupBy("business_id")
      .agg(
        avg($"stars").as("average")
      )
      .orderBy($"average".desc)
      .take(10)
      .map(e => (e.getAs[String]("business_id"), e.getAs[Double]("average")))

  val top10 = businessDf.filter($"business_id".isin(tmp.map(_._1): _*))
    .collect()
    .map(e => (e.getString(0), e.getString(1), e.getString(2)))

  sparkContext.parallelize(tmp.map { e =>
    (e._1, top10.filter(_._1 == e._1).map(_._2).head, top10.filter(_._1 == e._1).map(_._3), e._2)
  })
    .repartition(1)
    .saveAsTextFile(output)
}
