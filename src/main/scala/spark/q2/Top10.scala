package spark.q2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Top10 extends App {

  if(args.length == 0) {
    print("Need Arguments")
    System.exit(1)
  }

  private val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("top-10")
    .getOrCreate()

  private val sparkContext = sparkSession.sparkContext

  val input = args(0)
  val userInput = args(1)
  val output = args(2)

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
    (count, e._1)
  }
    .sortByKey(ascending = false, numPartitions = 1)
    .take(10)
    .map(e => (e._1, e._2))

  private val userData: RDD[(String, String)] = sparkContext.textFile(userInput).map { e =>
    val list = e.split(",")
    (list(0), list(1) + "\t" + list(2) + "\t" + list(3))
  }

  sparkContext.parallelize(red, numSlices = 1).map { e =>
    val usr1 = e._2.split("_")(0)
    val usr2 = e._2.split("_")(1)
    val user = userData.filter(u => List(usr1, usr2).contains(u._1)).take(2)
    (e._1, user.mkString("\t"))
  }.repartition(1).saveAsTextFile(output)

}
