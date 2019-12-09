import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashSet
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

  def run(data: RDD[Array[String]]): RDD[(Array[String], Long)] = {
    // NOTE: transactions should be cached
    val spark = data.sparkContext
    val numPartitions = data.getNumPartitions

    val transactions = data.map(_.toSet).cache()

    val cnt = data.count()
    val minSupportCnt = math.round(cnt * minSupport)

    // F-1 items
    var freqItems = data.flatMap { t =>
      t.toSet.toList.map{item: String => (HashSet(item), 1L)}
    }.reduceByKey(_ + _, numPartitions)
      .filter(_._2 >= minSupportCnt)

    var iter = 0
    var iterFreqItems = freqItems.collect()

    while (iterFreqItems.length > 0 && iter < maxIterations) {
      iter += 1

      logWarning(s"Iteration $iter: number of F-${iter+1} items: ${iterFreqItems.length}")

      val candidates = genCandidates(iterFreqItems, iter+1)
      val candidatesBC = spark.broadcast(candidates)

      val filteredFreqItems = transactions.mapPartitions { tranParts =>
        val candidates = candidatesBC.value

        tranParts.flatMap {tran =>
          var validCandidates = new ArrayBuffer[(HashSet[String], Long)]()
          for (cand <- candidates) {
            var valid = true
            for (item <- cand) {
              if (!tran.contains(item)) {
                valid = false
              }
            }
            if (valid) {
              validCandidates += Tuple2(cand, 1L)
            }
          }

          validCandidates
        }
      }.reduceByKey(_ + _, numPartitions).filter(_._2 >= minSupportCnt)

      freqItems = freqItems.union(filteredFreqItems)

      iterFreqItems = filteredFreqItems.collect()
      candidatesBC.destroy()
    }

    freqItems.map(tp => (tp._1.toArray, tp._2))
  }

 def genCandidates(freqItems: Array[(HashSet[String], Long)], k: Int): Array[HashSet[String]] = {
   var candidates = new ArrayBuffer[HashSet[String]]()
   val contained = new mutable.HashSet[String]

   for (i <- freqItems.indices) {
     for (j <- 0 until i) {
       val itemsSet = freqItems(i)._1.union(freqItems(j)._1)
       if (itemsSet.size == k) {
         val items: String = itemsSet.toArray.sorted.mkString(" ")
         if (!contained.contains(items)) {
           candidates += itemsSet
           contained.add(items)
         }
       }
     }
   }
   candidates.toArray
 }

}
