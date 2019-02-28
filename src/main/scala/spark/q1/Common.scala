package spark.q1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Common extends App {

  if(args.length == 0) {
    print("Need Arguments")
    System.exit(1)
  }

  private val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("common-friends-count")
    .getOrCreate()

  private val sparkContext = sparkSession.sparkContext

  private val input = args(0)
  private val output = args(1)

  val data: RDD[(String, Map[String, Int])] = sparkContext.textFile(input).filter(_.contains('\t')).map { e =>
    val tab = e.indexOf('\t')
    val id = e.substring(0, tab).toInt
    val list = e.substring(tab + 1)
    (id, list)
  }.flatMap {
    case (id, v) =>
      val friends = v.split(',')
      val mp = friends.flatMap(e => Map(e -> 1)).toMap
      friends.filter(!_.equals("")).map { e =>
        val fId = e.toInt
        if (fId > id)
          (s"${id}_$fId", mp)
        else
          (s"${fId}_$id", mp)
      }
  }

  private val red = data.reduceByKey {
    case (a, b) =>
      (a.keySet ++ b.keySet).map { i => (i, a.getOrElse(i, 0) + b.getOrElse(i, 0)) }.toMap
  }.map { e =>
    val count = e._2.count(_._2 == 2)
    (e._1, count)
  }.map(e => s"${e._1} : ${e._2}")

  red.repartition(1).saveAsTextFile(output)

}
