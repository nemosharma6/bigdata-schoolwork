package spark.mllib

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object CollaborativeFiltering extends App {

  private val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("collaborative-filtering")
    .getOrCreate()

  private val sparkContext = sparkSession.sparkContext

  val ratingData = args(0)
  val data = sparkContext.textFile(ratingData).randomSplit(Array(0.6, 0.4), seed = 12345L)
  val (trainingData, testData) = (data(0), data(1))
  val trainingRatings = trainingData.map(i => i.mkString.split("::")).filter(_.length == 4)
    .map(e => Rating(e(0).toInt, e(1).toInt, e(2).toDouble))

  val testRatings = testData.map(i => i.mkString.split("::")).filter(_.length == 4)
    .map(e => Rating(e(0).toInt, e(1).toInt, e(2).toDouble))

  val rank = 20
  val numIterations = 10
  val model = ALS.train(trainingRatings, rank, numIterations, 0.01)

  val usersProducts = testRatings.map {
    case Rating(user, product, _) => (user, product)
  }

  val predictions =
    model.predict(usersProducts).map {
      case Rating(user, product, rate) => ((user, product), rate)
    }

  val ratesAndPreds = testRatings.map {
    case Rating(user, product, rate) => ((user, product), rate)
  }.join(predictions)

  val MSE = ratesAndPreds.map { case ((_, _), (r1, r2)) => Math.pow(r1 - r2, 2) }.mean()
  println(s"Mean Squared Error = $MSE")
}
