import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    require(args.length >= 3)

    val input = args(0)
    val output = args(1)
    val minSupport = args(2).toDouble

    val conf = new SparkConf()
      .setAppName("SparkApriori")
//      .setMaster("local")
    val spark = new SparkContext(conf)
    spark.setLogLevel("WARN")

    val data = spark.textFile(input).map(line => line.split(" "))

    val apriori = new SparkApriori()
      .setMinSupport(minSupport)

    val freqItems = apriori.run(data)

    freqItems.map(_._1.toArray.mkString(" ")).saveAsTextFile(output)
  }

}
