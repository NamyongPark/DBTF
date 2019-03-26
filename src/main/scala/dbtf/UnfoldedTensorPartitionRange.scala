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
import scala.collection.mutable

/**
  * Given the lengths of row dimensions of matrix 1 and 2,
  * compute the partition id and partition split id to which each column index corresponds.
  * (partition id, partition split id) can be obtained by calling getPartitionInfo method.
  *
  * @param matrix1RowLength length of row dimension of matrix1
  * @param matrix2RowLength length of row dimension of matrix2
  * @param numUnfoldedTensorPartitions number of partitions of unfolded tensors
  * @param name optional name used for debugging
  */
@SerialVersionUID(100003)
class UnfoldedTensorPartitionRange(val matrix1RowLength: Int, val matrix2RowLength: Int, val numUnfoldedTensorPartitions: Int, val baseIndex: Int = 0, val name: String = "") extends Serializable {
  val rangeMap = new java.util.TreeMap[Long, (Int, Int)]()
  var partitionSplitInfoArray: Array[(Int, (Int, Int))] = _
  require(matrix1RowLength > 0 && matrix2RowLength > 0 && numUnfoldedTensorPartitions >= 1)

  def numPartitions: Int = numUnfoldedTensorPartitions

  private def updateRangeMap(): Unit = {
    DBTFDriver.log(s"Number of partitions of unfolded tensors=$numUnfoldedTensorPartitions ($name)")

    var partitionRangeStart, partitionRangeEnd = -1L
    val partitionSizeAverage: Double = matrix1RowLength * matrix2RowLength.toLong / numUnfoldedTensorPartitions.toDouble
    val partitionSizeAverageIntegerPart: Long = math.floor(partitionSizeAverage).toLong
    val partitionSizeAverageFractionalPart: Double = partitionSizeAverage - partitionSizeAverageIntegerPart
    var matrix2RangeStart0, matrix2RangeEnd0 = 0

    var fractionalSum = 0.0
    var uncoveredBlockLength = matrix2RowLength
    var matrix1RowIndex0 = 0
    var partitionSizeSum = 0L
    var partitionSplitId = 0
    val partitionSplitInfoMap = new mutable.LongMap[(Int, (Int, Int))]()

    for (partitionId <- 0 until numUnfoldedTensorPartitions) { // one core processes one partition.
      fractionalSum = fractionalSum + partitionSizeAverageFractionalPart
      // NOTE: floating point operation can introduce minor differences in fractionalSum at the last round.
      if (partitionId == numUnfoldedTensorPartitions - 1 && fractionalSum < 1 && partitionSizeAverageFractionalPart > 0) {
        assert(math.abs(1 - fractionalSum) < 0.00001)
        fractionalSum = 1.0
      }

      val partitionSize = {
        if (fractionalSum < 1) {
          partitionSizeAverageIntegerPart
        } else {
          fractionalSum -= 1
          partitionSizeAverageIntegerPart + 1
        }
      }
      partitionSizeSum += partitionSize

      if (partitionId == numUnfoldedTensorPartitions - 1) {
        assert(partitionSizeSum == matrix1RowLength * matrix2RowLength.toLong, s"$partitionSizeSum != ${matrix1RowLength * matrix2RowLength.toLong}")
      }

      partitionRangeStart = if (partitionRangeStart == -1) baseIndex else partitionRangeEnd + 1
      partitionRangeEnd = partitionRangeStart + partitionSize - 1
      assert(partitionRangeStart >= 0 && partitionRangeEnd > partitionRangeStart, s"partitionRangeStart: $partitionRangeStart, partitionRangeEnd: $partitionRangeEnd")
      assert(matrix2RangeStart0 >= 0 && matrix2RangeStart0 < matrix2RowLength, s"matrix2RangeStart0: $matrix2RangeStart0")

      if (partitionSize <= uncoveredBlockLength) { // current partition does not cross the block boundary.
        matrix2RangeEnd0 = matrix2RangeStart0 + partitionSize.toInt - 1
        assert(matrix2RangeEnd0 < matrix2RowLength)

        val partitionInfo = (partitionId, partitionSplitId)
        partitionSplitInfoMap.put(partitionSplitId, (matrix1RowIndex0, (matrix2RangeStart0, matrix2RangeEnd0)))
        rangeMap.put(partitionRangeStart, partitionInfo)
        rangeMap.put(partitionRangeEnd, partitionInfo)
        partitionSplitId += 1

        uncoveredBlockLength -= partitionSize.toInt
        if (uncoveredBlockLength == 0) {
          uncoveredBlockLength = matrix2RowLength // reset uncoveredBlockLength
          matrix1RowIndex0 += 1 // move to the next block
        }
        matrix2RangeStart0 = (matrix2RangeEnd0 + 1) % matrix2RowLength
      } else {
        assert(uncoveredBlockLength > 0, s"uncoveredBlockLength: $uncoveredBlockLength")
        val firstSplitLength = uncoveredBlockLength
        val remainingSplitLength: Long = partitionSize - uncoveredBlockLength

        // first split
        matrix2RangeEnd0 = matrix2RowLength - 1
        assert(matrix2RangeEnd0 - matrix2RangeStart0 + 1 == uncoveredBlockLength)
        uncoveredBlockLength -= firstSplitLength
        assert(uncoveredBlockLength == 0)

        var partitionInfo = (partitionId, partitionSplitId)
        partitionSplitInfoMap.put(partitionSplitId, (matrix1RowIndex0, (matrix2RangeStart0, matrix2RangeEnd0)))
        rangeMap.put(partitionRangeStart, partitionInfo)
        rangeMap.put(partitionRangeStart + firstSplitLength - 1, partitionInfo)
        partitionSplitId += 1

        matrix1RowIndex0 += 1 // move to the next block
        partitionRangeStart += firstSplitLength

        // middle full-size split(s) if any
        val numMiddleSplits = (remainingSplitLength / matrix2RowLength).toInt
        for (i <- 0 until numMiddleSplits) {
          val splitLength = matrix2RowLength
          matrix2RangeStart0 = 0
          matrix2RangeEnd0 = splitLength - 1

          partitionInfo = (partitionId, partitionSplitId)
          partitionSplitInfoMap.put(partitionSplitId, (matrix1RowIndex0, (matrix2RangeStart0, matrix2RangeEnd0)))
          rangeMap.put(partitionRangeStart, partitionInfo)
          rangeMap.put(partitionRangeStart + splitLength - 1, partitionInfo)
          partitionSplitId += 1

          partitionRangeStart += splitLength
          matrix1RowIndex0 += 1 // move to the next block
        }

        uncoveredBlockLength = matrix2RowLength
        val lastSplitLength = (remainingSplitLength % matrix2RowLength).toInt
        // last split
        if (lastSplitLength > 0) {
          assert(lastSplitLength < matrix2RowLength)
          assert(partitionRangeStart + lastSplitLength - 1 == partitionRangeEnd)
          matrix2RangeStart0 = 0
          matrix2RangeEnd0 = matrix2RangeStart0 + lastSplitLength - 1

          partitionInfo = (partitionId, partitionSplitId)
          partitionSplitInfoMap.put(partitionSplitId, (matrix1RowIndex0, (matrix2RangeStart0, matrix2RangeEnd0)))
          rangeMap.put(partitionRangeStart, partitionInfo)
          rangeMap.put(partitionRangeStart + lastSplitLength - 1, partitionInfo)
          partitionSplitId += 1

          uncoveredBlockLength -= lastSplitLength
          if (uncoveredBlockLength == 0) {
            uncoveredBlockLength = matrix2RowLength // reset uncoveredBlockLength
            matrix1RowIndex0 += 1 // move to the next block
          }
        }

        matrix2RangeStart0 = (matrix2RangeEnd0 + 1) % matrix2RowLength
      }
    }

    // convert partitionSplitInfoMap into an array
    assert(partitionSplitId == partitionSplitInfoMap.size)
    partitionSplitInfoArray = new Array[(Int, (Int, Int))](partitionSplitId)
    for (i <- 0 until partitionSplitId) {
      partitionSplitInfoArray(i) = partitionSplitInfoMap(i)
    }

    assert(matrix2RangeStart0 == 0, s"matrix2RangeStart0: $matrix2RangeStart0")
    assert(matrix1RowIndex0 == matrix1RowLength, s"matrix1RowIndex0: $matrix1RowIndex0")
    assert(matrix2RangeEnd0 == matrix2RowLength - 1, s"matrix2RangeEnd0: $matrix2RangeEnd0")
    assert(partitionRangeEnd - baseIndex == matrix2RowLength * matrix1RowLength.toLong - 1)
  }

  updateRangeMap()

  /**
    * Return a tuple of (partitionId, partitionSplitId) to which the given index belongs.
    *
    * @param index index
    * @return (partitionId, partitionSplitId)
    */
  def getPartitionInfo(index: Long): (Int, Int) = {
    val floorEntry: Option[Entry[Long, (Int, Int)]] = Option(rangeMap.floorEntry(index))
    val ceilingEntry: Option[Entry[Long, (Int, Int)]] = Option(rangeMap.ceilingEntry(index))
    assert(floorEntry.isDefined, s"index: $index")
    assert(ceilingEntry.isDefined, s"index: $index")
    assert(floorEntry.get.getValue == ceilingEntry.get.getValue, s"name: $name, index: $index, ${floorEntry.get.getValue} != ${ceilingEntry.get.getValue}")
    floorEntry.get.getValue
  }

  def getPartitionSplitInfo(partitionSplitId: Int): (Int, (Int, Int)) = {
    partitionSplitInfoArray(partitionSplitId)
  }

  /**
    * Helper method to obtain the partition Id to which the given index belongs.
    *
    * @param index index
    * @return partition Id
    */
  def getPartitionId(index: Long): Int = {
    val (partitionId, _) = getPartitionInfo(index)
    partitionId
  }
}
