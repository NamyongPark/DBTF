/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package dbtf

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Array-based partitioner class for unfolded tensor
  */
object UnfoldedTensorArrayBasedPartitioner {
  def partition(sc: SparkContext, unfoldedTensorRDD: RDD[(Int, Long)], rowDimLength: Int,
                partitionRangeByCol: UnfoldedTensorPartitionRange, matrix2RowLength: Int, baseIndex: Int = 0): RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])] = {
    val _createCombiner = createCombiner(rowDimLength, baseIndex, matrix2RowLength, _: (Int, Long, Int))
    val _mergeValue = mergeValue(rowDimLength, baseIndex, matrix2RowLength, _: mutable.LongMap[Array[ArrayBuffer[Int]]], _: (Int, Long, Int))
    val brPartitionRangeByCol: Broadcast[UnfoldedTensorPartitionRange] = sc.broadcast(partitionRangeByCol)

    val partitioned: RDD[(Int, mutable.LongMap[Array[ArrayBuffer[Int]]])] = unfoldedTensorRDD map {
      case (rowIndex, colIndex) =>
        val partitionRangeByCol = brPartitionRangeByCol.value
        val (partitionId, partitionSplitId) = partitionRangeByCol.getPartitionInfo(colIndex)
        (partitionId, (rowIndex, colIndex, partitionSplitId))
    } combineByKey(_createCombiner, _mergeValue, mergeCombiners, new IdentityPartitioner(partitionRangeByCol.numPartitions), true, null)

    // Array[(partitionId, Array[((matrix1RowIndex0, (matrix2RangeStart, matrix2RangeEnd)), Array[Array[Int]])])]
    partitioned mapValues {
      (groupedPartRowwiseMap: mutable.LongMap[Array[ArrayBuffer[Int]]]) =>
        val t = groupedPartRowwiseMap mapValues {
          (partRowwiseArray: Array[ArrayBuffer[Int]]) => partRowwiseArray.map {
            rowPiece => if (rowPiece == null) {
              null
            } else {
              rowPiece.toArray.sorted
            }
          }
        }

        t.toArray.map {
          case (partitionSplitId, partRowwiseArray) =>
            (partitionRangeByCol.partitionSplitInfoArray(partitionSplitId.toInt), partRowwiseArray)
        }
    }
  }

  def createRowwiseArray(rowDimLength: Int): Array[ArrayBuffer[Int]] = {
    val rowwiseArray = new Array[ArrayBuffer[Int]](rowDimLength)
    rowwiseArray
  }

  def createCombiner(rowDimLength: Int, baseIndex: Int, matrix2RowLength: Int, value: (Int, Long, Int)): mutable.LongMap[Array[ArrayBuffer[Int]]] = {
    val (rowIndex, colIndex, partitionSplitId) = value
    val groupedPartRowwiseMap = mutable.LongMap.empty[Array[ArrayBuffer[Int]]] // key: partitionSplitId
    val partRowwiseArray = groupedPartRowwiseMap.getOrElseUpdate(partitionSplitId, createRowwiseArray(rowDimLength))

    if (partRowwiseArray(rowIndex - baseIndex) == null) {
      partRowwiseArray(rowIndex - baseIndex) = ArrayBuffer[Int]((colIndex % matrix2RowLength).toInt)
    } else {
      partRowwiseArray(rowIndex - baseIndex) += (colIndex % matrix2RowLength).toInt
    }

    groupedPartRowwiseMap
  }

  def mergeValue(rowDimLength: Int, baseIndex: Int, matrix2RowLength: Int, groupedPartRowwiseMap: mutable.LongMap[Array[ArrayBuffer[Int]]],
                 value: (Int, Long, Int)): mutable.LongMap[Array[ArrayBuffer[Int]]] = {
    val (rowIndex, colIndex, partitionSplitId) = value
    val partRowwiseArray = groupedPartRowwiseMap.getOrElseUpdate(partitionSplitId, createRowwiseArray(rowDimLength)) // key: partitionSplitId

    if (partRowwiseArray(rowIndex - baseIndex) == null) {
      partRowwiseArray(rowIndex - baseIndex) = ArrayBuffer[Int]((colIndex % matrix2RowLength).toInt)
    } else {
      partRowwiseArray(rowIndex - baseIndex) += (colIndex % matrix2RowLength).toInt
    }

    groupedPartRowwiseMap
  }

  def mergeCombiners(groupedPartRowwiseMap1: mutable.LongMap[Array[ArrayBuffer[Int]]],
                     groupedPartRowwiseMap2: mutable.LongMap[Array[ArrayBuffer[Int]]]): mutable.LongMap[Array[ArrayBuffer[Int]]] = {
    val (mergeTo, mergeFrom) = (groupedPartRowwiseMap1, groupedPartRowwiseMap2)

    mergeFrom foreach {
      case (partitionSplitId: Long, mergeFromPartRowwiseArray: Array[ArrayBuffer[Int]]) =>
        if (mergeTo.isDefinedAt(partitionSplitId)) {
          val mergeToPartRowwiseArray: Array[ArrayBuffer[Int]] = mergeTo(partitionSplitId)
          // copy entries in mergeFrom to mergeTo
          for (rowIndex0 <- mergeFromPartRowwiseArray.indices) {
            val mergeFromRowPiece: ArrayBuffer[Int] = mergeFromPartRowwiseArray(rowIndex0)
            if (mergeFromRowPiece != null) {
              if (mergeToPartRowwiseArray(rowIndex0) == null) {
                mergeToPartRowwiseArray(rowIndex0) = mergeFromRowPiece
              } else {
                mergeToPartRowwiseArray(rowIndex0) ++= mergeFromRowPiece
              }
            }
          }
        } else {
          mergeTo(partitionSplitId) = mergeFrom(partitionSplitId)
        }
    }

    mergeTo
  }

  def countEntries(groupedPartRowwiseMap: mutable.LongMap[Array[ArrayBuffer[Int]]]): Int = {
    var num: Int = 0
    groupedPartRowwiseMap.values foreach {
      (partRowwiseArray: Array[ArrayBuffer[Int]]) =>
        // println("[" + partRowwiseArray.mkString(", ") + "]")
        partRowwiseArray foreach {
          case rowPiece if rowPiece != null => num += rowPiece.length
        }
    }
    num
  }
}