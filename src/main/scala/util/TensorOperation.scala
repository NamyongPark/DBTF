/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package util

import java.io.{File, PrintWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import MatrixOperation.sparseToSparseRowwiseMatrix

/**
  * Object for various tensor operations
  */
object TensorOperation {
  /**
    * Parse the text lines for sparse tensor data, and return an RDD of a 3-way sparse tensor.
    * Lines starting with '#' or '%' are considered comments, and ignored.
    *
    * @param sc spark context
    * @param tensorLinesRDD RDD of text lines of sparse tensor data
    * @param sepRegex regular expression for separator
    * @return RDD of a 3-way sparse tensor
    */
  def parseTensorCheckingComments(sc: SparkContext, tensorLinesRDD: RDD[String], sepRegex: String = ","): RDD[(Int, Int, Int)] = {
    tensorLinesRDD.flatMap { line =>
      val sp = line.split(sepRegex)
      if (line.startsWith("#") || line.startsWith("%")) {
        None
      } else {
        assert(sp.length == 3, s"invalid tensor line: $line")
        Some((sp(0).toInt, sp(1).toInt, sp(2).toInt))
      }
    }
  }

  /**
    * Parse the text lines for sparse tensor data, and return an RDD of a 3-way sparse tensor.
    * Lines starting with '#' or '%' are considered comments, and ignored.
    *
    * @param sc spark context
    * @param tensorLinesRDD RDD of text lines of sparse tensor data
    * @param sepRegex regular expression for separator
    * @return RDD of a 3-way sparse tensor
    */
  def parseTensor(sc: SparkContext, tensorLinesRDD: RDD[String], sepRegex: String = ","): RDD[(Int, Int, Int)] = {
    tensorLinesRDD.map { line =>
      val sp = line.split(sepRegex)
      assert(sp.length == 3, s"invalid tensor line: $line")
      (sp(0).toInt, sp(1).toInt, sp(2).toInt)
    }
  }

  def convertToBase0Tensor(tensorRDD: RDD[(Int, Int, Int)], baseIndex: Int): RDD[(Int, Int, Int)] = {
    assert(List(0, 1).contains(baseIndex), s"Invalid tensor base index: $baseIndex")
    if (baseIndex == 0) tensorRDD
    else {
      tensorRDD.map { case (i, j, k) => (i - baseIndex, j - baseIndex, k - baseIndex) }
    }
  }

  /**
    * Unfold a 3-way tensor RDD into a sparse matrix RDD.
    *
    * @param tensorRDD  3-way Boolean tensor RDD
    * @param modeLengths length of each mode
    * @param mode       mode into which to unfold a tensor
    * @return unfolded matrix
    */
  def unfoldTensorRDD(tensorRDD: RDD[(Int, Int, Int)], modeLengths: (Int, Int, Int))
                     (mode: Int): RDD[(Int, Long)] = {
    val (modeI, modeJ, _) = modeLengths

    mode match {
      case 1 => tensorRDD.map { case (i, j, k) => (i, j + k * modeJ.toLong) }
      case 2 => tensorRDD.map { case (i, j, k) => (j, i + k * modeI.toLong) }
      case 3 => tensorRDD.map { case (i, j, k) => (k, i + j * modeI.toLong) }
      case _ => throw new IllegalArgumentException(s"invalid mode: $mode. mode must be between 1 and 3 (both inclusive).")
    }
  }

  /**
    * Unfold a 3-way tensor RDD into a sparse matrix RDD.
    *
    * @param tensor      3-way Boolean tensor
    * @param modeLengths length of each mode
    * @param mode        mode into which to unfold a tensor
    * @return unfolded matrix
    */
  def unfoldTensor(tensor: Array[(Int, Int, Int)], modeLengths: (Int, Int, Int))
                  (mode: Int): Array[(Int, Long)] = {
    val (modeI, modeJ, _) = modeLengths

    mode match {
      case 1 => tensor.map { case (i, j, k) => (i, j + k * modeJ.toLong) }
      case 2 => tensor.map { case (i, j, k) => (j, i + k * modeI.toLong) }
      case 3 => tensor.map { case (i, j, k) => (k, i + j * modeI.toLong) }
      case _ => throw new IllegalArgumentException(s"invalid mode: $mode. mode must be between 1 and 3 (both inclusive).")
    }
  }

  /**
    * Split the range of the given mode into an array of smaller ranges such that
    * the number of elements belonging to each split is approximately the same.
    * It is assumed that the nonzeros are distributed in such a way that allows
    * the achievement of this goal.
    *
    * @param tensorRDD 3-way Boolean tensor RDD
    * @param mode mode whose range is to be split
    * @param numSplits number of splits
    * @return an array of non-overlapping subranges
    */
  def splitRange(tensorRDD: RDD[(Int, Int, Int)], mode: Int, numSplits: Int): (Array[(Int, Int)], Array[Int]) = {
    assert(Array(1, 2, 3).contains(mode), s"Invalid modeIndex=$mode")
    val numTotalNonzeros = tensorRDD.count()
    var minSplitSize: Int = (numTotalNonzeros.toDouble / numSplits).toInt

    val numNonzerosByIndex = mode match {
      case 1 =>
        tensorRDD.map(x => (x._1, 1)).reduceByKey(_ + _).collectAsMap()
      case 2 =>
        tensorRDD.map(x => (x._2, 1)).reduceByKey(_ + _).collectAsMap()
      case 3 =>
        tensorRDD.map(x => (x._3, 1)).reduceByKey(_ + _).collectAsMap()
    }
    val indices = numNonzerosByIndex.keys.toArray.sorted

    val ranges = mutable.ArrayBuffer.empty[(Int, Int)]
    val numNonzerosBySplits = mutable.ArrayBuffer.empty[Int]
    var modeIndex = indices.head
    var i = 0
    var accumNumNonzeros = 0
    breakable {
      while (modeIndex <= indices.last) {
        accumNumNonzeros += numNonzerosByIndex(modeIndex)

        if (accumNumNonzeros >= minSplitSize) {
          val rangeStartIndex = if (ranges.nonEmpty) ranges.last._2 + 1 else 0
          ranges.append((rangeStartIndex, modeIndex))
          numNonzerosBySplits.append(accumNumNonzeros)
          accumNumNonzeros = 0
          val remainingSplits = numSplits - numNonzerosBySplits.length
          if (remainingSplits > 0) {
            minSplitSize = ((numTotalNonzeros - numNonzerosBySplits.sum).toDouble / remainingSplits).toInt
          } else {
            assert(i == indices.length - 1, s"i: $i")
          }
        }

        i += 1
        if (i >= indices.length) {
          assert(modeIndex == indices.last)
          break
        }
        modeIndex = indices(i)
      }
    }

    assert(numNonzerosBySplits.sum == numTotalNonzeros, numNonzerosBySplits.mkString(","))
    assert(ranges.length == numSplits, ranges.mkString(","))

    (ranges.toArray, numNonzerosBySplits.toArray)
  }

  /**
    * Compute the sum of absolute distance between two tensors.
    *
    * @param sc          spark context
    * @param tensor1RDD  3-way Boolean tensor RDD
    * @param tensor2RDD  3-way Boolean tensor RDD
    * @return distance (sum of absolute difference) between two tensors
    */
  def computeSumAbsDiff(sc: SparkContext, tensor1RDD: RDD[(Int, Int, Int)], tensor2RDD: RDD[(Int, Int, Int)]): Double = {
    val accum = sc.doubleAccumulator("Sum of Absolute Difference Accumulator")

    tensor1RDD.map(t => (t, 1)) cogroup tensor2RDD.map(t => (t, 1)) foreach {
      case ((_, (it1, it2))) =>
        assert(it1.isEmpty || it1.size == 1, s"it1: [${it1.mkString(", ")}]")
        assert(it2.isEmpty || it2.size == 1, s"it2: [${it2.mkString(", ")}]")
        val it1Val = it1.headOption.getOrElse(0).toDouble
        val it2Val = it2.headOption.getOrElse(0).toDouble
        accum.add(TensorDistanceMeasure.sumAbsDiff.distance(it1Val, it2Val))
    }
    accum.value
  }

  /**
    * Compute the sum of absolute distance between two tensors.
    *
    * @param tensor1 3-way Boolean tensor
    * @param tensor2 3-way Boolean tensor
    * @return distance (sum of absolute difference) between two tensors
    */
  def computeSumAbsDiff(tensor1: Set[(Int, Int, Int)], tensor2: Set[(Int, Int, Int)]): Int = {
    tensor1.diff(tensor2).size + tensor2.diff(tensor1).size
  }

  /**
    * Generate a 3-way Boolean tensor from the given factor matrices.
    *
    * @param sc            spark context
    * @param factorMatrix1 Boolean sparse factor matrix for mode 1
    * @param factorMatrix2 Boolean sparse factor matrix for mode 2
    * @param factorMatrix3 Boolean sparse factor matrix for mode 3
    * @return 3-way Boolean tensor RDD
    */
  def generateTensorRDDFromFactors(sc: SparkContext, factorMatrix1: Array[(Int, Int)], factorMatrix2: Array[(Int, Int)],
                                   factorMatrix3: Array[(Int, Int)], baseIndex: Int = 0): RDD[(Int, Int, Int)] = {
    val factorMatrix1RDD = sc.parallelize(factorMatrix1)
    val factorMatrix2RDD = sc.parallelize(factorMatrix2)
    val factorMatrix3RDD = sc.parallelize(factorMatrix3)

    val factorMat1ByCol = factorMatrix1RDD map { case (row, col) => (col, (1, row)) }
    val factorMat2ByCol = factorMatrix2RDD map { case (row, col) => (col, (2, row)) }
    val factorMat3ByCol = factorMatrix3RDD map { case (row, col) => (col, (3, row)) }
    val factorsByCol = factorMat1ByCol union factorMat2ByCol union factorMat3ByCol

    factorsByCol.groupByKey().flatMap { // compute outer product for each group of n-th columns
      case (col: Int, it: Iterable[(Int, Int)]) =>
        val entriesByMode: Map[Int, Iterable[(Int, Int)]] = it.groupBy(_._1)
        val entriesFactorMat1: Option[Iterable[(Int, Int)]] = entriesByMode.get(1)
        val entriesFactorMat2: Option[Iterable[(Int, Int)]] = entriesByMode.get(2)
        val entriesFactorMat3: Option[Iterable[(Int, Int)]] = entriesByMode.get(3)
        if (entriesFactorMat1.nonEmpty && entriesFactorMat2.nonEmpty && entriesFactorMat3.nonEmpty) {
            for (entry1 <- entriesFactorMat1.get; entry2 <- entriesFactorMat2.get; entry3 <- entriesFactorMat3.get) yield {
              (entry1._2 + baseIndex, entry2._2 + baseIndex, entry3._2 + baseIndex)
            }
        } else {
          None
        }
    } distinct() // for Boolean summation
  }

  def buildApproximateBooleanTensor(coreTensor: Set[(Int, Int, Int)], A: Array[(Int, Int)], dimA: (Int, Int),
                                    B: Array[(Int, Int)], dimB: (Int, Int), C: Array[(Int, Int)], dimC: (Int, Int),
                                    baseIndex: Int): Set[(Int, Int, Int)] = {
    val sparseRowwiseA: Array[Array[Int]] = sparseToSparseRowwiseMatrix(A, dimA._1)
    val sparseRowwiseB: Array[Array[Int]] = sparseToSparseRowwiseMatrix(B, dimB._1)
    val sparseRowwiseC: Array[Array[Int]] = sparseToSparseRowwiseMatrix(C, dimC._1)

    val approxTensorIndices = ArrayBuffer.empty[(Int, Int, Int)]

    for ((rowA, i0) <- sparseRowwiseA.zipWithIndex if !rowA.isEmpty) {
      for ((rowB, j0) <- sparseRowwiseB.zipWithIndex if !rowB.isEmpty) {
        for ((rowC, k0) <- sparseRowwiseC.zipWithIndex if !rowC.isEmpty) {
          // add (i,j,k) to the approximate tensor
          breakable {
            for (alpha <- rowA; beta <- rowB; gamma <- rowC) {
              if (coreTensor.contains((alpha, beta, gamma))) {
                approxTensorIndices.append((i0 + baseIndex, j0 + baseIndex, k0 + baseIndex))
                break
              }
            }
          }
        }
      }
    }

    approxTensorIndices.toSet
  }

  /**
    * Generate an n-mode random tensor in a sparse format.
    *
    * @param modeLengths array of mode lengths
    * @param density density (= number of non-zeros / product of all mode lengths)
    * @param baseIndex base index
    * @param random random number generator
    * @return n-mode random tensor
    */
  def generateRandomTensor(modeLengths: Array[Int], density: Double, baseIndex: Int, random: scala.util.Random = scala.util.Random): Array[Array[Int]] = {
    assert(density > 0 && density <= 1.0, s"density: $density")
    val totalElements = modeLengths.foldLeft(density) { case (accum, value) => accum * value }.toInt
    assert(totalElements > 0, s"totalElements: $totalElements, modeLengths: ${modeLengths.mkString(", ")}")

    val numModes = modeLengths.length
    val randomSparseTensor = new Array[Array[Int]](totalElements)
    val genKeySet = mutable.HashSet.empty[String]
    while (genKeySet.size < totalElements) {
      val element = new Array[Int](numModes)
      for (i <- element.indices) {
        element(i) = random.nextInt(modeLengths(i)) + baseIndex
      }
      val elStr = element.mkString(",")

      if (!genKeySet.contains(elStr)) {
        randomSparseTensor(genKeySet.size) = element
        genKeySet += elStr
      }
    }

    randomSparseTensor
  }

  /**
    * Generate a three-mode random tensor in a sparse format
    *
    * @param modeLengths ternary tuple of mode lengths
    * @param density density (= number of non-zeros / product of all mode lengths)
    * @param baseIndex base index
    * @param random random number generator
    * @return n-mode random tensor
    */
  def generateRandomThreeWayTensor(modeLengths: (Int, Int, Int), density: Double, baseIndex: Int, random: scala.util.Random = scala.util.Random): Array[(Int, Int, Int)] = {
    val _modeLengths = Array(modeLengths._1, modeLengths._2, modeLengths._3)
    val _randomTensor: Array[Array[Int]] = generateRandomTensor(_modeLengths, density, baseIndex, random)
    _randomTensor.map(entries => (entries(0), entries(1), entries(2)))
  }

  /**
    * Generate a random matrix in a sparse format
    *
    * @param modeLengths binary tuple of mode lengths
    * @param density density (= number of non-zeros / product of all mode lengths)
    * @param baseIndex base index
    * @param random random number generator
    * @return n-mode random tensor
    */
  def generateRandomMatrix(modeLengths: (Int, Int), density: Double, baseIndex: Int, random: scala.util.Random = scala.util.Random): Array[(Int, Int)] = {
    val _modeLengths = Array(modeLengths._1, modeLengths._2)
    val _randomTensor: Array[Array[Int]] = generateRandomTensor(_modeLengths, density, baseIndex, random)
    _randomTensor.map(entries => (entries(0), entries(1)))
  }

  /**
    * Print the given tensor at the specified file using the given separator.
    *
    * @param sparseTensor tensor in a sparse format
    * @param outputFilePath path to the output file
    * @param separator separator between tensor entries
    */
  def outputTensor(sparseTensor: Array[Array[Int]], outputFilePath: String, separator: String = ","): Unit = {
    val writer = new PrintWriter(new File(outputFilePath))
    sparseTensor.foreach {
      t => writer.write(t.mkString(separator) + "\n")
    }
    writer.close()
  }
}

sealed trait TensorDistanceMeasure { def distance(v1: Double, v2: Double): Double }
object TensorDistanceMeasure {
  // The sum of absolute differences (or L1 error)
  case object sumAbsDiff extends TensorDistanceMeasure {
    override def distance(v1: Double, v2: Double): Double = math.abs(v1 - v2)
  }
  // The squared Frobenius norm of a difference tensor
  case object squaredFrobenius extends TensorDistanceMeasure {
    override def distance(v1: Double, v2: Double): Double = math.pow(v1 - v2, 2)
  }
}
