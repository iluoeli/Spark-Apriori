import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkAprioriSuite extends FunSuite with BeforeAndAfterAll {

  var  spark: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
    spark = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("base") {

  }

}
