/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package dbtf

import java.util.Map.Entry

import org.apache.spark.rdd.RDD

import scala.collection.mutable

@SerialVersionUID(100007)
class InputTensorPartitionRange(val tensorRDD: RDD[(Int, Int, Int)], val modeLengths: (Int, Int, Int),
                                val numInputTensorPartitions: Int) extends Serializable {
  require(modeLengths._1 > 0 && modeLengths._2 > 0 && modeLengths._3 > 0 && numInputTensorPartitions >= 1, (modeLengths, numInputTensorPartitions))
  private val rangeMap = new java.util.TreeMap[Long, Int]()
  private val partitionInfoMap = mutable.LongMap.empty[((Int, Int), (Int, Int), (Int, Int))]

  private def updateRangeMap(): Unit = {
    val I = modeLengths._1
    val J = modeLengths._2
    val K = modeLengths._3

    DBTFDriver.log(s"Number of partitions of the input tensor=$numInputTensorPartitions")
    if (numInputTensorPartitions > I) {
      throw new Exception(s"ERROR: Try again with a smaller value for the '--num-partitions' parameter.")
    }

    val ranges = DBTF.splitRangeWithNumSplits(0, I - 1, numInputTensorPartitions)

    assert(ranges.length == numInputTensorPartitions, s"ranges.length=${ranges.length}, numPartitions=$numInputTensorPartitions")
    for ((r, partitionId) <- ranges.zipWithIndex) {
      rangeMap.put(r._1, partitionId)
      rangeMap.put(r._2, partitionId)
      val partitionInfo: ((Int, Int), (Int, Int), (Int, Int)) = (r, (0, J - 1), (0, K - 1))
      partitionInfoMap.put(partitionId, partitionInfo)
    }
  }

  updateRangeMap()

  /**
    * Return partition Id to which the given index belongs.
    *
    * @param index 0-based index
    * @return partition Id
    */
  def getPartitionId(index: (Int, Int, Int)): Int = {
    val (i0, _, _) = index
    val floorEntry: Option[Entry[Long, Int]] = Option(rangeMap.floorEntry(i0))
    val ceilingEntry: Option[Entry[Long, Int]] = Option(rangeMap.ceilingEntry(i0))
    assert(floorEntry.isDefined, s"index: $index")
    assert(ceilingEntry.isDefined, s"index: $index")
    assert(floorEntry.get.getValue == ceilingEntry.get.getValue, s"index: $index, ${floorEntry.get.getValue} != ${ceilingEntry.get.getValue}")
    floorEntry.get.getValue
  }

  /**
    * Return a partition info of the given partition Id, which is a tuple of (range for the 1st mode, range for the 2nd mode, range for the 3rd mode).
    * Both end points of a range are inclusive.
    *
    * @param partitionId partition Id
    * @return (range for the 1st mode, range for the 2nd mode, range for the 3rd mode) for the given partition Id
    */
  def getPartitionInfo(partitionId: Int): ((Int, Int), (Int, Int), (Int, Int)) = {
    partitionInfoMap(partitionId)
  }

  def getPartitionIds: Array[Long] = {
    partitionInfoMap.keys.toArray
  }
}
