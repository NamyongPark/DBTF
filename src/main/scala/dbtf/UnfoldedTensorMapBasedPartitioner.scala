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

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer

/**
  * Map-based partitioner class for unfolded tensor
  */
object UnfoldedTensorMapBasedPartitioner {
  /**
    * Return partitioned unfolded tensor.
    * An unfolded tensor is split into partitions of (approximately) equal size,
    * which are further split into blocks according to the underlying pointwise vector-matrix products.
    *
    * @param unfoldedTensorRDD RDD of an unfolded tensor
    * @param rowDimLength length of row dimension
    * @param partitionRangeByCol object for partitioning range by column
    * @param matrix2RowLength row length of matrix2
    * @param baseIndex base index of a tensor
    * @return RDD of partitioned unfolded tensor
    */
  def partition(sc: SparkContext, unfoldedTensorRDD: RDD[(Int, Long)], rowDimLength: Int,
                partitionRangeByCol: UnfoldedTensorPartitionRange, matrix2RowLength: Int, baseIndex: Int = 0): RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])] = {
    val _createCombiner = createCombiner(rowDimLength, baseIndex, matrix2RowLength, _: (Int, Long, Int))
    val _mergeValue = mergeValue(rowDimLength, baseIndex, matrix2RowLength, _: mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]], _: (Int, Long, Int))
    val _mergeCombiners = mergeCombiners(rowDimLength, _: mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]], _: mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]])
    val brPartitionRangeByCol: Broadcast[UnfoldedTensorPartitionRange] = sc.broadcast(partitionRangeByCol)

    val partitioned: RDD[(Int, mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]])] = unfoldedTensorRDD map {
      case (rowIndex, colIndex) =>
        val partitionRangeByCol = brPartitionRangeByCol.value
        val (partitionId, partitionSplitId) = partitionRangeByCol.getPartitionInfo(colIndex)
        (partitionId, (rowIndex, colIndex, partitionSplitId))
    } combineByKey(_createCombiner, _mergeValue, _mergeCombiners, new IdentityPartitioner(partitionRangeByCol.numPartitions), true, null)

    // Array[(partitionId, Array[((matrix1RowIndex0, (matrix2RangeStart, matrix2RangeEnd)), Map[Long, Array[Int]])])]
    partitioned mapValues {
      (groupedPartRowwiseMap: mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]]) =>
        val t = groupedPartRowwiseMap mapValues {
          (partRowwiseMap: mutable.LongMap[ArrayBuffer[Int]]) => partRowwiseMap.mapValues {
            rowPiece => rowPiece.toArray.sorted
          }
        }

        t.toArray.map {
          case (partitionSplitId, partRowwiseMap) =>
            (partitionRangeByCol.partitionSplitInfoArray(partitionSplitId.toInt), partRowwiseMap)
        }
    }
  }

  def createRowwiseMap(): mutable.LongMap[ArrayBuffer[Int]] = {
    val rowwiseMap = mutable.LongMap.empty[ArrayBuffer[Int]] // key: rowIndex0
    rowwiseMap
  }

  def createCombiner(rowDimLength: Int, baseIndex: Int, matrix2RowLength: Int, value: (Int, Long, Int)): mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]] = {
    val (rowIndex, colIndex, partitionSplitId) = value
    val groupedPartRowwiseMap = mutable.LongMap.empty[mutable.LongMap[ArrayBuffer[Int]]] // key: partitionSplitId
    val partRowwiseMap = groupedPartRowwiseMap.getOrElseUpdate(partitionSplitId, createRowwiseMap())

    val partRowwise = partRowwiseMap.get(rowIndex - baseIndex)
    if (partRowwise.isDefined) {
      partRowwise.get += (colIndex % matrix2RowLength).toInt
    } else {
      partRowwiseMap.put(rowIndex - baseIndex, ArrayBuffer[Int]((colIndex % matrix2RowLength).toInt))
    }

    groupedPartRowwiseMap
  }

  def mergeValue(rowDimLength: Int, baseIndex: Int, matrix2RowLength: Int, groupedPartRowwiseMap: mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]],
                 value: (Int, Long, Int)): mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]] = {
    val (rowIndex, colIndex, partitionSplitId) = value
    val partRowwiseMap = groupedPartRowwiseMap.getOrElseUpdate(partitionSplitId, createRowwiseMap()) // key: partitionSplitId

    val partRowwise = partRowwiseMap.get(rowIndex - baseIndex)
    if (partRowwise.isDefined) {
      partRowwise.get += (colIndex % matrix2RowLength).toInt
    } else {
      partRowwiseMap.put(rowIndex - baseIndex, ArrayBuffer[Int]((colIndex % matrix2RowLength).toInt))
    }

    groupedPartRowwiseMap
  }

  def mergeCombiners(rowDimLength: Int, groupedPartRowwiseMap1: mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]],
                     groupedPartRowwiseMap2: mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]]): mutable.LongMap[mutable.LongMap[ArrayBuffer[Int]]] = {
    val (mergeTo, mergeFrom) = (groupedPartRowwiseMap1, groupedPartRowwiseMap2)

    mergeFrom foreach {
      case (partitionSplitId: Long, mergeFromPartRowwiseMap: mutable.LongMap[ArrayBuffer[Int]]) =>
        if (mergeTo.isDefinedAt(partitionSplitId)) {
          val mergeToPartRowwiseMap: mutable.LongMap[ArrayBuffer[Int]] = mergeTo(partitionSplitId)
          // copy entries in mergeFrom to mergeTo
          for (rowIndex0 <- 0 until rowDimLength) {
            val mergeFromRowPiece: Option[ArrayBuffer[Int]] = mergeFromPartRowwiseMap.get(rowIndex0)
            if (mergeFromRowPiece.isDefined) {
              if (mergeToPartRowwiseMap.isDefinedAt(rowIndex0)) {
                mergeToPartRowwiseMap(rowIndex0) ++= mergeFromRowPiece.get
              } else {
                mergeToPartRowwiseMap(rowIndex0) = mergeFromRowPiece.get
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
        partRowwiseArray foreach {
          case rowPiece if rowPiece != null => num += rowPiece.length
        }
    }
    num
  }
}