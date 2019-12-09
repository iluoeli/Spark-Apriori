import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SparkApriori(
    private var minSupport: Double,
    private var maxIterations: Int) extends  Serializable with Logging{

  def this() = this(0.8, 5)

  def getMinSupport: Double =minSupport

  def setMinSupport(value: Double): this.type = {
    require(value > 0,
      "the minimun support should > 0, but got $value")
    this.minSupport = value
    this
  }

  def getMaxIterations: Int = maxIterations

  def setMaxIterations(value: Int): this.type = {
    require(value > 0,"" +
      "the number of max iterations should >0, but got $value")
    this.maxIterations = value
    this
  }

  def run(transactions: RDD[Array[Int]]): RDD[Array[Int]] = {
    // NOTE: transactions should be cached
    val spark = transactions.sparkContext
    val numPartitions = transactions.getNumPartitions

    val cnt = transactions.count()
    val minSupportCnt = math.round(cnt * minSupport)

    // F-1 items
    val freq1Items = transactions.flatMap { t =>
      t.toSet.map(item => (item, 1L))
    }.reduceByKey(_ + _, numPartitions)
      .filter(_._2 >= minSupportCnt)
      .collect()
      .sortBy(_._1)

    var iter = 0
    var iterFreqItems = freq1Items.map{case (item, freq) => item.toString}

    while (iterFreqItems.length > 0 && iter < maxIterations) {
      iter += 1

      val candidates = genCandidates(iterFreqItems, iter+1)
      val candidatesBC = spark.broadcast(candidates)

      val filteredFreqItems = transactions.mapPartitions { tranParts =>
        val candidates = candidatesBC.value

        tranParts.flatMap {
          val itemsCnts = new Array[Long](candidates.length)
          val set = new mutable.HashSet[String]()
          set.subsets()

          null
        }
      }
    }

    null
  }

 def genCandidates(freqItems: Array[String], k: Int): Array[String] = {
   val freqItemsSet = freqItems.toSet
   val preFreqItems = freqItems.map(_.split(" ").map(_.toInt).toSet)

   var nextFreqItems = new ArrayBuffer[String]()

   for (i <- preFreqItems.indices) {
     for (j <- 0 until i) {
       val itemsSet = preFreqItems(i).union(preFreqItems(j))
       if (itemsSet.size == k) {
         val items: String = itemsSet.toArray.sortBy(_).mkString(" ")
         if (freqItemsSet.contains(items)) {
//            nextFreqItem += items
         }
       }
     }
   }

   null
 }

}
