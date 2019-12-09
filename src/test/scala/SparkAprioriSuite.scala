import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkAprioriSuite extends FunSuite with BeforeAndAfterAll {

  var spark: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("SparkAprioriSuite")
    spark = new SparkContext(conf)
    spark.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("base") {
    val data = spark.parallelize(Seq(
      Array(1, 2, 3, 4),
      Array(1, 2, 3),
      Array(1, 2),
      Array(1)
    )).map(_.map(_.toString))

    val apriori = new SparkApriori()
      .setMinSupport(0.5)

    val freqItems = apriori.run(data).collect()

    freqItems.foreach(x => println(x._1.mkString(",") + "\t\t" + x._2))
  }

}
