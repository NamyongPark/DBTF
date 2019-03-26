/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package util

import java.util.{BitSet => JBitSet}

import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
  * Object for various matrix operations
  */
object MatrixOperation {
  /**
    * Transpose the given matrix RDD
    *
    * @param sparseMatrixRDD sparse matrix RDD
    * @return RDD of transposed matrix in a sparse format
    */
  def transposeRDD(sparseMatrixRDD: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    sparseMatrixRDD.map(_.swap)
  }

  /**
    * Transpose the given matrix
    *
    * @param sparseMatrix sparse matrix
    * @return transposed matrix in a sparse format
    */
  def transpose(sparseMatrix: Array[(Int, Int)]): Array[(Int, Int)] = {
    sparseMatrix.map(_.swap)
  }

  def initTwoDimArray(rowDimension: Int, colDimension: Int): Array[Array[Int]] = {
    val twoDimArr = new Array[Array[Int]](rowDimension)
    for (i <- twoDimArr.indices) {
      twoDimArr(i) = new Array[Int](colDimension)
    }
    twoDimArr
  }

  /**
    * Transform the given sparse matrix into a rowwise matrix
    *
    * @param sparseMatrix sparse matrix
    * @param rowDimension row dimension
    * @param baseIndex base index
    * @return a rowwise matrix corresponding to the given matrix
    */
  def rowwiseMatrix(sparseMatrix: Array[(Int, Int)], rowDimension: Int, baseIndex: Int = 0): Array[Array[Int]] = {
    val rowGrouped: Map[Int, Array[(Int, Int)]] = sparseMatrix.groupBy(_._1)
    val rowwiseMatrix = new Array[Array[Int]](rowDimension)
    for (rowIndex0 <- 0 until rowDimension) {
      val matrixEntry = rowGrouped.get(rowIndex0 + baseIndex)
      rowwiseMatrix(rowIndex0) = matrixEntry.fold(Array[Int]()) { (entryArray: Array[(Int, Int)]) => entryArray map (_._2) }.sorted // discard row component and sort each row
    }

    assert({ rowwiseMatrix foreach(row => assert(row.sorted sameElements row, row.mkString(", "))); true})
    rowwiseMatrix
  }

  /**
    * Transform the given sparse matrix into a sparse rowwise matrix
    *
    * @param sparseMatrix sparse matrix
    * @param rowDimension row dimension
    * @return a sparse rowwise matrix corresponding to the given matrix
    */
  def sparseToSparseRowwiseMatrix(sparseMatrix: Array[(Int, Int)], rowDimension: Int): Array[Array[Int]] = {
    val rowGrouped: Map[Int, Array[(Int, Int)]] = sparseMatrix.groupBy(_._1)
    val rowwiseMatrix = new Array[Array[Int]](rowDimension)
    for (rowIndex0 <- 0 until rowDimension) {
      val rowEntriesOption: Option[Array[(Int, Int)]] = rowGrouped.get(rowIndex0)
      rowwiseMatrix(rowIndex0) = rowEntriesOption.fold(Array[Int]()) { (rowEntries: Array[(Int, Int)]) => rowEntries map (_._2) }.sorted // discard row component and sort each row
    }

    assert({ rowwiseMatrix foreach(row => assert(row.sorted sameElements row, row.mkString(", "))); true})
    rowwiseMatrix
  }

  /**
    * Transform the given sparse matrix into a columnwise sparse matrix
    *
    * @param sparseMatrix sparse matrix
    * @param colDimension column dimension
    * @param baseIndex base index
    * @return a sparse columnwise matrix corresponding to the given matrix
    */
  def sparseToColumnwiseSparseMatrix(sparseMatrix: Array[(Int, Int)], colDimension: Int, baseIndex: Int = 0): Array[Array[Int]] = {
    val columnwiseSparseMatrix = new Array[Array[Int]](colDimension)
    val colGrouped: Map[Int, Array[(Int, Int)]] = sparseMatrix.groupBy(_._2)
    for (colIndex0 <- 0 until colDimension) {
      val colEntriesOption: Option[Array[(Int, Int)]] = colGrouped.get(colIndex0 + baseIndex)
      columnwiseSparseMatrix(colIndex0) = colEntriesOption.fold(Array[Int]()) { (colEntries: Array[(Int, Int)]) => colEntries.map(_._1) }.sorted // discard column component and sort each row
    }

    assert({ columnwiseSparseMatrix foreach(col => assert(col.sorted sameElements col, col.mkString(", "))); true})
    columnwiseSparseMatrix
  }

  /**
    * Transform the given dense matrix into a sparse matrix
    *
    * @param denseMatrix dense matrix
    * @param baseIndex base index
    * @return sparse matrix corresponding to the given dense matrix
    */
  def denseToSparseMatrix(denseMatrix: Array[Array[Byte]], baseIndex: Int): Array[(Int, Int)] = {
    denseMatrix.zipWithIndex flatMap {
      case ((row, rowIndex0)) => row.zipWithIndex flatMap {
        case (value, colIndex0) => if (value != 0) Some((rowIndex0 + baseIndex, colIndex0 + baseIndex)) else None
      }
    }
  }

  /**
    * Transform the given sparse matrix into a dense matrix
    *
    * @param sparseMatrix sparse matrix
    * @param matrixDimension row and column dimensions of a matrix
    * @param baseIndex base index
    * @return dense matrix corresponding to the given sparse matrix
    */
  def sparseToDenseMatrix(sparseMatrix: Array[(Int, Int)], matrixDimension: (Int, Int), baseIndex: Int = 0): Array[Array[Byte]] = {
    val (rowDim, colDim) = matrixDimension
    val denseMatrix = Array.ofDim[Byte](rowDim, colDim)
    sparseMatrix foreach {
      case (rowIndex, colIndex) => denseMatrix(rowIndex - baseIndex)(colIndex - baseIndex) = 1
    }
    denseMatrix
  }

  /**
    * Transform a sparse matrix into a bit row matrix (a rowwise matrix where each row is represented using a BitSet).
    *
    * @param sparseMatrix sparse matrix
    * @param matrixDimension row and column dimensions of a matrix
    * @param singleBitSetArray array of single BitSets
    * @param baseIndex base index
    * @return a bit row matrix corresponding to a sparse matrix
    */
  def sparseToBitRowMatrix(sparseMatrix: Array[(Int, Int)], matrixDimension: (Int, Int), singleBitSetArray: Array[JBitSet], baseIndex: Int = 0): Array[JBitSet] = {
    val (rowDim, colDim) = matrixDimension

    val bitRowMatrix = new Array[JBitSet](rowDim)
    val rowGrouped: Map[Int, Array[(Int, Int)]] = sparseMatrix.groupBy(_._1)
    val colDim0 = colDim - 1
    for (rowIndex0 <- 0 until rowDim) {
      val matrixRowEntries = rowGrouped.get(rowIndex0 + baseIndex)

      bitRowMatrix(rowIndex0) = matrixRowEntries.fold(new JBitSet()) { (rowEntries: Array[(Int, Int)]) =>
        val bitSet = new JBitSet()
        rowEntries foreach { e =>
          val colIndex0 = e._2 - baseIndex
          bitSet.or(singleBitSetArray(colDim0 - colIndex0))
        }
        bitSet
      }
    }

    bitRowMatrix
  }

  /**
    * Transform a bit row matrix (a rowwise matrix where each row is represented using a BitSet) into a sparse matrix.
    *
    * @param bitRowMatrix rowwise bit matrix
    * @param bitRowLength length of bit row
    * @param baseIndex base index
    * @return a sparse matrix corresponding to a bit row matrix
    */
  def bitRowToSparseMatrix(bitRowMatrix: Array[JBitSet], bitRowLength: Int, baseIndex: Int = 0): Array[(Int, Int)] = {
    bitRowMatrix.zipWithIndex flatMap {
      case (bitRow: JBitSet, rowIndex0: Int) =>
        assert(bitRow.length() <= bitRowLength, s"bitRow: $bitRow, bitRowLength: $bitRowLength")

        val rowEntries = ArrayBuffer[(Int, Int)]()
        var i = bitRow.nextSetBit(0)
        while (i >= 0) {
          val colIndex0 = (bitRowLength - 1) - i
          rowEntries += ((rowIndex0 + baseIndex, colIndex0 + baseIndex))
          i = bitRow.nextSetBit(i + 1)
        }
        rowEntries
    }
  }

  def numUniqueColumns(columnwiseSparseMatrix: Array[Array[Int]]): Int = {
    val uniqueColumns = mutable.HashSet.empty[Set[Int]]
    val columnwise: Array[Set[Int]] = columnwiseSparseMatrix.map(x => x.toSet)
    for (c <- columnwise) {
      uniqueColumns.add(c)
    }
    uniqueColumns.size
  }
}
