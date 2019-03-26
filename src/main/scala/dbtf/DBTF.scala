/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package dbtf

import java.text.SimpleDateFormat
import java.util.{Calendar, BitSet => JBitSet}

import dbtf.DBTFDriver.log
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import util.{MatrixOperation, TensorOperation}

import scala.collection.{Map, mutable}
import scala.util.control.Breaks
import scala.util.control.Breaks._

object DBTF {
  var random = new scala.util.Random()
  val DEBUG = false

  object Convergence extends Enumeration {
    type Method = Value
    val BY_ABS_ERROR_DELTA, BY_RECONSTRUCTION_ERROR, BY_NUM_BIT_UPDATES, BY_NUM_ITERATION = Value
  }

  /**
    * Perform a distributed Boolean CP factorization on the given tensor RDD, and return three Boolean factor matrices.
    *
    * @param sc spark context
    * @param tensorRDD sparse Boolean tensor RDD
    * @param modeLengths lengths of three modes
    * @param rank rank
    * @param randomSeed random seed
    * @param numUnfoldedTensorPartitions number of partitions of unfolded tensors
    * @param numInitialCandidates number of initial candidates
    * @param initialFactorMatrixDensity density of initial factor matrices
    * @param convergeByNumIters whether to converge by the number of iterations
    * @param convergeByNumItersValue the number of iteration at which to converge
    * @param convergeByAbsErrorDeltaValue absolute error delta for convergence
    * @param numConsecutiveItersForConvergeByAbsErrorDelta number of consecutive iterations for converging by absolute error delta
    * @param convergenceMethod method for convergence
    * @param probForZeroForTieBreaking probability to choose zero when several column values (including zero) have the same error delta
    * @param maxZeroPercentage maximum percentage of zeros in a factor matrix
    * @param maxRankSplitSize maximum number of rows out of which to build a single cache table
    * @param computeError a parameter to specify when to compute a reconstruction error
    * @param numColsToUpdate number of columns to update at once
    * @return three Boolean factor matrices
    */
  def cpFactorization(sc: SparkContext,
                      tensorRDD: RDD[(Int, Int, Int)],
                      modeLengths: (Int, Int, Int),
                      rank: Int,
                      randomSeed: Int,
                      unfoldedTensorPartitioner: String,
                      numUnfoldedTensorPartitions: Int,
                      numInitialCandidates: Int,
                      initialFactorMatrixDensity: Double,
                      convergeByNumIters: Boolean,
                      convergeByNumItersValue: Int,
                      convergeByAbsErrorDeltaValue: Double,
                      numConsecutiveItersForConvergeByAbsErrorDelta: Int,
                      convergenceMethod: Convergence.Method = Convergence.BY_NUM_ITERATION,
                      probForZeroForTieBreaking: Double,
                      maxZeroPercentage: Double,
                      maxRankSplitSize: Int,
                      computeError: String,
                      numColsToUpdate: Int = 1): (Array[(Int, Int)], Array[(Int, Int)], Array[(Int, Int)], Array[Double], Long) = {
    if (randomSeed >= 0) {
      random = new scala.util.Random(randomSeed)
    }

    /** Unfold the tensor into each mode. */
    val (i, j, k) = modeLengths
    val unfoldedTensorMode1RDD = TensorOperation.unfoldTensorRDD(tensorRDD, modeLengths)(mode = 1) // i-by-jk
    val unfoldedTensorMode2RDD = TensorOperation.unfoldTensorRDD(tensorRDD, modeLengths)(mode = 2) // j-by-ik
    val unfoldedTensorMode3RDD = TensorOperation.unfoldTensorRDD(tensorRDD, modeLengths)(mode = 3) // k-by-ij

    var A: Array[(Int, Int)] = Array[(Int, Int)]()
    var B: Array[(Int, Int)] = Array[(Int, Int)]()
    var C: Array[(Int, Int)] = Array[(Int, Int)]()
    val dimA = (i, rank)
    val dimB = (j, rank)
    val dimC = (k, rank)

    // Partition unfolded tensors and cache them.
    val partitionRange1 = new UnfoldedTensorPartitionRange(matrix1RowLength = k, matrix2RowLength = j, numUnfoldedTensorPartitions, name = "partition range 1")
    val partitionRange2 = new UnfoldedTensorPartitionRange(matrix1RowLength = k, matrix2RowLength = i, numUnfoldedTensorPartitions, name = "partition range 2")
    val partitionRange3 = new UnfoldedTensorPartitionRange(matrix1RowLength = j, matrix2RowLength = i, numUnfoldedTensorPartitions, name = "partition range 3")

    val partUnfoldedTensorMode1MapBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        Option(UnfoldedTensorMapBasedPartitioner.partition(sc, unfoldedTensorMode1RDD, i, partitionRange1, matrix2RowLength = j).setName("Partitioned unfolded tensor - mode1").cache())
      } else {
        None
      }
    val partUnfoldedTensorMode1ArrayBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        None
      } else {
        Option(UnfoldedTensorArrayBasedPartitioner.partition(sc, unfoldedTensorMode1RDD, i, partitionRange1, matrix2RowLength = j).setName("Partitioned unfolded tensor - mode1").cache())
      }

    val partUnfoldedTensorMode2MapBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        Option(UnfoldedTensorMapBasedPartitioner.partition(sc, unfoldedTensorMode2RDD, j, partitionRange2, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode2").cache())
      } else {
        None
      }
    val partUnfoldedTensorMode2ArrayBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        None
      } else {
        Option(UnfoldedTensorArrayBasedPartitioner.partition(sc, unfoldedTensorMode2RDD, j, partitionRange2, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode2").cache())
      }

    val partUnfoldedTensorMode3MapBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        Option(UnfoldedTensorMapBasedPartitioner.partition(sc, unfoldedTensorMode3RDD, k, partitionRange3, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode3").cache())
      } else {
        None
      }
    val partUnfoldedTensorMode3ArrayBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        None
      } else {
        Option(UnfoldedTensorArrayBasedPartitioner.partition(sc, unfoldedTensorMode3RDD, k, partitionRange3, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode3").cache())
      }

    val singleBitArray: Array[JBitSet] = prepareSingleBitArray(bitLength = rank)
    val tablesOfNumZeros = Array(1 to numColsToUpdate: _*).map { n => buildTableOfNumZerosInBinaryForm(n) }

    // Update factors until convergence.
    var iter = 1
    var converged = false
    var numConsecutiveIters = 0
    var sumErrorDelta: Option[Long] = None
    val reconstructionErrors = new mutable.ArrayBuffer[Double]()
    var totalTimeFactors = 0L

    do {
      DBTFDriver.log(s"\nStart of Iteration-$iter")
      val t0 = System.nanoTime()
      var timeFactors: Long = 0

      if (iter == 1) { // update N sets of initial factor matrices and find the best one
        /** Initialize factor matrices A, B, C. */
        val initialAs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val initialBs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val initialCs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val updatedAs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val updatedBs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val updatedCs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val numBitUpdatesArr: Array[Long] = new Array[Long](numInitialCandidates)
        val sumErrorDeltaArr: Array[Long] = new Array[Long](numInitialCandidates)

        for (idx <- 0 until numInitialCandidates) {
          DBTFDriver.log(s"Generating initial factor matrix set-$idx...")
          initialAs(idx) = initializeFactorMatrixRandomly((i, rank), initialFactorMatrixDensity, random = DBTF.random)
          DBTFDriver.log(s"\tNumber of nonzeros in initial A-$idx: ${initialAs(idx).length} (${initialAs(idx).length / (dimA._1 * dimA._2).toDouble})")
          initialBs(idx) = initializeFactorMatrixRandomly((j, rank), initialFactorMatrixDensity, random = DBTF.random)
          DBTFDriver.log(s"\tNumber of nonzeros in initial B-$idx: ${initialBs(idx).length} (${initialBs(idx).length / (dimB._1 * dimB._2).toDouble})")
          initialCs(idx) = initializeFactorMatrixRandomly((k, rank), initialFactorMatrixDensity, random = DBTF.random)
          DBTFDriver.log(s"\tNumber of nonzeros in initial C-$idx: ${initialCs(idx).length} (${initialCs(idx).length / (dimC._1 * dimC._2).toDouble})")
          DBTFDriver.log(s"Initial factor matrix set-$idx generated.")

          val t0Factor = System.nanoTime()
          DBTFDriver.log(s"Updating initial factor matrix set-$idx...")
          val (updatedA, updatedB, updatedC, _numBitUpdates, _sumErrorDelta) = updateAllFactorMatricesCP(
            sc, initialAs(idx), (i, rank), initialBs(idx), (j, rank), initialCs(idx), (k, rank),
            partUnfoldedTensorMode1MapBasedRDD, partUnfoldedTensorMode2MapBasedRDD, partUnfoldedTensorMode3MapBasedRDD,
            partUnfoldedTensorMode1ArrayBasedRDD, partUnfoldedTensorMode2ArrayBasedRDD, partUnfoldedTensorMode3ArrayBasedRDD,
            partitionRange1, partitionRange2, partitionRange3, rank, singleBitArray, numUnfoldedTensorPartitions, numColsToUpdate,
            probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
          )
          DBTFDriver.log(s"Initial factor matrix set-$idx updated.")
          timeFactors += (System.nanoTime() - t0Factor)
          totalTimeFactors += timeFactors

          updatedAs(idx) = updatedA
          updatedBs(idx) = updatedB
          updatedCs(idx) = updatedC
          numBitUpdatesArr(idx) = _numBitUpdates
          sumErrorDeltaArr(idx) = _sumErrorDelta
        }

        val minIndex = sumErrorDeltaArr.zipWithIndex.min._2
        A = updatedAs(minIndex)
        B = updatedBs(minIndex)
        C = updatedCs(minIndex)
        sumErrorDelta = Some(sumErrorDeltaArr(minIndex))

        val t1 = System.nanoTime()
        DBTFDriver.log(s"End of Iteration-$iter")
        DBTFDriver.log(s" - Elapsed time: ${(t1 - t0) / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Elapsed time (updating factors): ${timeFactors / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Number of bit updates: ${numBitUpdatesArr.mkString(", ")}")
        DBTFDriver.log(s" - Sum of error delta: ${sumErrorDeltaArr.mkString(", ")}")
        DBTFDriver.log(s" - Min. sum of error delta: ${sumErrorDelta.get}")
      } else {
        val t0Factor = System.nanoTime()
        val (updatedA, updatedB, updatedC, _numBitUpdates, _sumErrorDelta) = updateAllFactorMatricesCP(
          sc, A, (i, rank), B, (j, rank), C, (k, rank),
          partUnfoldedTensorMode1MapBasedRDD, partUnfoldedTensorMode2MapBasedRDD, partUnfoldedTensorMode3MapBasedRDD,
          partUnfoldedTensorMode1ArrayBasedRDD, partUnfoldedTensorMode2ArrayBasedRDD, partUnfoldedTensorMode3ArrayBasedRDD,
          partitionRange1, partitionRange2, partitionRange3, rank, singleBitArray, numUnfoldedTensorPartitions, numColsToUpdate,
          probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
        )
        timeFactors += (System.nanoTime() - t0Factor)
        totalTimeFactors += timeFactors

        A = updatedA
        B = updatedB
        C = updatedC
        sumErrorDelta = Some(_sumErrorDelta)

        val t1 = System.nanoTime()
        DBTFDriver.log(s"End of Iteration-$iter")
        DBTFDriver.log(s" - Elapsed time: ${(t1 - t0) / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Elapsed time (updating factors): ${timeFactors / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Number of bit updates: ${_numBitUpdates}")
        DBTFDriver.log(s" - Sum of error delta: ${_sumErrorDelta}")
      }

      if (computeError == DBTFDriver.COMPUTE_ERROR_FOR_EVERY_ITER) {
        reconstructionErrors += computeReconstructionError(sc, DBTFDriver.FACTORIZATION_METHOD_CP, tensorRDD, A, B, C, dimA, dimB, dimC, None)
      }

      /** Start of checking for convergence */
      convergenceMethod match {
        case Convergence.BY_ABS_ERROR_DELTA =>
          if (math.abs(sumErrorDelta.get) < convergeByAbsErrorDeltaValue) {
            numConsecutiveIters += 1
          } else {
            numConsecutiveIters = 0
          }
          if (numConsecutiveIters >= numConsecutiveItersForConvergeByAbsErrorDelta) {
            converged = true
          }
          if (convergeByNumIters && iter == convergeByNumItersValue) {
            converged = true
          }

          iter += 1
        case Convergence.BY_NUM_ITERATION =>
          if (iter == convergeByNumItersValue) {
            converged = true
          }

          iter += 1
        case Convergence.BY_NUM_BIT_UPDATES =>
          throw new IllegalArgumentException
      }
      /** End of checking for convergence */
    } while (!converged)

    partUnfoldedTensorMode1MapBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode2MapBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode3MapBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode1ArrayBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode2ArrayBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode3ArrayBasedRDD.foreach(_.unpersist(blocking = false))

    (A, B, C, reconstructionErrors.toArray, totalTimeFactors)
  }

  /**
    * Perform a distributed Boolean Tucker factorization on the given tensor RDD, and return three Boolean factor matrices and a core tensor.
    *
    * @param sc spark context
    * @param tensorRDD sparse Boolean tensor RDD
    * @param modeLengths lengths of three modes
    * @param ranks ranks for three modes (dimensions of each mode of a core tensor)
    * @param randomSeed random seed
    * @param numUnfoldedTensorPartitions number of partitions of unfolded tensors
    * @param numInputTensorPartitions number of partitions of the input tensor
    * @param numInitialCandidates number of initial candidates
    * @param initialFactorMatrixDensity density of initial factor matrices
    * @param initialCoreTensorDensity density of an initial core tensor
    * @param convergeByNumIters whether to converge by the number of iterations
    * @param convergeByNumItersValue the number of iteration at which to converge
    * @param convergeByAbsErrorDeltaValue absolute error delta for convergence
    * @param numConsecutiveItersForConvergeByAbsErrorDelta number of consecutive iterations for converging by absolute error delta
    * @param convergenceMethod method for convergence
    * @param probForZeroForTieBreaking probability to choose zero when several column values (including zero) have the same error delta
    * @param maxZeroPercentage maximum percentage of zeros in a factor matrix
    * @param maxRankSplitSize maximum number of rows out of which to build a single cache table
    * @param computeError a parameter to specify when to compute a reconstruction error
    * @param numColsToUpdate number of columns to update at once
    * @return three Boolean factor matrices and a core tensor
    */
  def tuckerFactorization(sc: SparkContext,
                          tensorRDD: RDD[(Int, Int, Int)],
                          modeLengths: (Int, Int, Int),
                          ranks: (Int, Int, Int),
                          randomSeed: Int,
                          unfoldedTensorPartitioner: String,
                          numUnfoldedTensorPartitions: Int,
                          numInputTensorPartitions: Int,
                          numInitialCandidates: Int,
                          initialFactorMatrixDensity: Double,
                          initialCoreTensorDensity: Double,
                          convergeByNumIters: Boolean,
                          convergeByNumItersValue: Int,
                          convergeByAbsErrorDeltaValue: Double,
                          numConsecutiveItersForConvergeByAbsErrorDelta: Int,
                          convergenceMethod: Convergence.Method = Convergence.BY_NUM_ITERATION,
                          probForZeroForTieBreaking: Double,
                          maxZeroPercentage: Double,
                          maxRankSplitSize: Int,
                          computeError: String,
                          numColsToUpdate: Int = 1): (Array[(Int, Int)], Array[(Int, Int)], Array[(Int, Int)], Option[Array[(Int, Int, Int)]], Array[Double], Long, Long) = {
    if (randomSeed >= 0) {
      random = new scala.util.Random(randomSeed)
    }

    /** Unfold the tensor into each mode. */
    val (i, j, k) = modeLengths
    val unfoldedTensorMode1RDD = TensorOperation.unfoldTensorRDD(tensorRDD, modeLengths)(mode = 1) // i-by-jk
    val unfoldedTensorMode2RDD = TensorOperation.unfoldTensorRDD(tensorRDD, modeLengths)(mode = 2) // j-by-ik
    val unfoldedTensorMode3RDD = TensorOperation.unfoldTensorRDD(tensorRDD, modeLengths)(mode = 3) // k-by-ij

    var A: Array[(Int, Int)] = Array[(Int, Int)]()
    var B: Array[(Int, Int)] = Array[(Int, Int)]()
    var C: Array[(Int, Int)] = Array[(Int, Int)]()
    var coreTensor: Array[(Int, Int, Int)] = Array[(Int, Int, Int)]()
    val dimA = (i, ranks._1)
    val dimB = (j, ranks._2)
    val dimC = (k, ranks._3)

    // Partition unfolded tensors and cache them.
    val partitionRange1 = new UnfoldedTensorPartitionRange(matrix1RowLength = k, matrix2RowLength = j, numUnfoldedTensorPartitions, name = "partition range 1")
    val partitionRange2 = new UnfoldedTensorPartitionRange(matrix1RowLength = k, matrix2RowLength = i, numUnfoldedTensorPartitions, name = "partition range 2")
    val partitionRange3 = new UnfoldedTensorPartitionRange(matrix1RowLength = j, matrix2RowLength = i, numUnfoldedTensorPartitions, name = "partition range 3")

    val partUnfoldedTensorMode1MapBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        Option(UnfoldedTensorMapBasedPartitioner.partition(sc, unfoldedTensorMode1RDD, i, partitionRange1, matrix2RowLength = j).setName("Partitioned unfolded tensor - mode1").cache())
      } else {
        None
      }
    val partUnfoldedTensorMode1ArrayBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        None
      } else {
        Option(UnfoldedTensorArrayBasedPartitioner.partition(sc, unfoldedTensorMode1RDD, i, partitionRange1, matrix2RowLength = j).setName("Partitioned unfolded tensor - mode1").cache())
      }

    val partUnfoldedTensorMode2MapBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        Option(UnfoldedTensorMapBasedPartitioner.partition(sc, unfoldedTensorMode2RDD, j, partitionRange2, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode2").cache())
      } else {
        None
      }
    val partUnfoldedTensorMode2ArrayBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        None
      } else {
        Option(UnfoldedTensorArrayBasedPartitioner.partition(sc, unfoldedTensorMode2RDD, j, partitionRange2, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode2").cache())
      }

    val partUnfoldedTensorMode3MapBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        Option(UnfoldedTensorMapBasedPartitioner.partition(sc, unfoldedTensorMode3RDD, k, partitionRange3, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode3").cache())
      } else {
        None
      }
    val partUnfoldedTensorMode3ArrayBasedRDD =
      if (unfoldedTensorPartitioner == DBTFDriver.UNFOLDED_TENSOR_PARTITIONER_MAP_BASED) {
        None
      } else {
        Option(UnfoldedTensorArrayBasedPartitioner.partition(sc, unfoldedTensorMode3RDD, k, partitionRange3, matrix2RowLength = i).setName("Partitioned unfolded tensor - mode3").cache())
      }

    // Partition and cache the input tensor.
    val partitionRange = new InputTensorPartitionRange(tensorRDD, modeLengths, numInputTensorPartitions)
    val brPartitionRange: Broadcast[InputTensorPartitionRange] = sc.broadcast(partitionRange)
    val partInputTensorRDD = InputTensorPartitioner.partitionInputTensor(tensorRDD, brPartitionRange).setName("Partitioned input tensor").cache()

    val singleBitArrays: Array[Array[JBitSet]] = Array(ranks._1, ranks._2, ranks._3).map(r => prepareSingleBitArray(bitLength = r))
    val tablesOfNumZeros = Array(1 to numColsToUpdate: _*).map { n => buildTableOfNumZerosInBinaryForm(n) }

    // Update factors and a core until convergence.
    var iter = 1
    var converged = false
    var numConsecutiveIters = 0
    var sumErrorDelta: Option[Long] = None
    val reconstructionErrors = new mutable.ArrayBuffer[Double]()
    var totalTimeFactors = 0L
    var totalTimeCore = 0L

    do {
      DBTFDriver.log(s"\nStart of Iteration-$iter")
      val t0 = System.nanoTime()
      var iterTimeFactors: Long = 0
      var iterTimeCore: Long = 0

      if (iter == 1) { // update N sets of initial factor matrices and core tensors, and find the best one
        /** Initialize factor matrices A, B, C. */
        val initialAs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val initialBs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val initialCs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val updatedAs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val updatedBs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val updatedCs: Array[Array[(Int, Int)]] = new Array[Array[(Int, Int)]](numInitialCandidates)
        val coreTensors: Array[Array[(Int, Int, Int)]] = new Array[Array[(Int, Int, Int)]](numInitialCandidates)
        for (i <- coreTensors.indices) {
          coreTensors(i) = Array.empty[(Int, Int, Int)]
        }
        val numFactorMatrixBitUpdatesArr: Array[Long] = new Array[Long](numInitialCandidates)
        val numCoreTensorBitUpdatesArr: Array[Long] = new Array[Long](numInitialCandidates)
        val sumErrorDeltaArr: Array[Long] = new Array[Long](numInitialCandidates)

        for (idx <- 0 until numInitialCandidates) {
          DBTFDriver.log(s"Generating initial factor matrix set-$idx...")
          initialAs(idx) = initializeFactorMatrixRandomly(dimA, initialFactorMatrixDensity, random = DBTF.random)
          DBTFDriver.log(s"\tNumber of nonzeros in initial A-$idx: ${initialAs(idx).length} (${initialAs(idx).length / (dimA._1 * dimA._2).toDouble})")
          initialBs(idx) = initializeFactorMatrixRandomly(dimB, initialFactorMatrixDensity, random = DBTF.random)
          DBTFDriver.log(s"\tNumber of nonzeros in initial B-$idx: ${initialBs(idx).length} (${initialBs(idx).length / (dimB._1 * dimB._2).toDouble})")
          initialCs(idx) = initializeFactorMatrixRandomly(dimC, initialFactorMatrixDensity, random = DBTF.random)
          DBTFDriver.log(s"\tNumber of nonzeros in initial C-$idx: ${initialCs(idx).length} (${initialCs(idx).length / (dimC._1 * dimC._2).toDouble})")
          DBTFDriver.log(s"Initial factor matrix set-$idx generated.")

          val t0Core = System.nanoTime()
          DBTFDriver.log(s"Updating core tensor $idx...")
          val (updatedCoreTensor, numCoreTensorBitUpdates) = updateCoreTensor(sc, initCoreTensor(ranks, initialCoreTensorDensity),
            ranks, partInputTensorRDD, brPartitionRange, initialAs(idx), dimA, initialBs(idx), dimB, initialCs(idx), dimC)
          coreTensors(idx) = updatedCoreTensor
          numCoreTensorBitUpdatesArr(idx) = numCoreTensorBitUpdates
          DBTFDriver.log(s"Core tensor $idx updated.")
          val timeCore = System.nanoTime() - t0Core
          iterTimeCore += timeCore
          totalTimeCore += timeCore

          val t0Factor = System.nanoTime()
          DBTFDriver.log(s"Updating initial factor matrix set-$idx...")
          val (updatedA, updatedB, updatedC, _numFactorMatrixBitUpdates, _sumErrorDelta) = updateAllFactorMatricesTucker(
            sc, coreTensors(idx), initialAs(idx), dimA, initialBs(idx), dimB, initialCs(idx), dimC,
            partUnfoldedTensorMode1MapBasedRDD, partUnfoldedTensorMode2MapBasedRDD, partUnfoldedTensorMode3MapBasedRDD,
            partUnfoldedTensorMode1ArrayBasedRDD, partUnfoldedTensorMode2ArrayBasedRDD, partUnfoldedTensorMode3ArrayBasedRDD,
            partitionRange1, partitionRange2, partitionRange3, singleBitArrays, numUnfoldedTensorPartitions, numColsToUpdate,
            probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
          )
          DBTFDriver.log(s"Initial factor matrix set-$idx updated.")
          val timeFactors = System.nanoTime() - t0Factor
          iterTimeFactors += timeFactors
          totalTimeFactors += timeFactors

          updatedAs(idx) = updatedA
          updatedBs(idx) = updatedB
          updatedCs(idx) = updatedC
          numFactorMatrixBitUpdatesArr(idx) = _numFactorMatrixBitUpdates
          sumErrorDeltaArr(idx) = _sumErrorDelta
        }

        val minIndex = sumErrorDeltaArr.zipWithIndex.min._2
        A = updatedAs(minIndex)
        B = updatedBs(minIndex)
        C = updatedCs(minIndex)
        coreTensor = coreTensors(minIndex)
        sumErrorDelta = Some(sumErrorDeltaArr(minIndex))

        val t1 = System.nanoTime()
        DBTFDriver.log(s"End of Iteration-$iter")
        DBTFDriver.log(s" - Elapsed time: ${(t1 - t0) / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Elapsed time (updating factors): ${iterTimeFactors / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Elapsed time (updating a core): ${iterTimeCore / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Number of bit updates in factor matrices: ${numFactorMatrixBitUpdatesArr.mkString(", ")}")
        DBTFDriver.log(s" - Number of bit updates in the core tensor: ${numCoreTensorBitUpdatesArr.mkString(", ")}")
        DBTFDriver.log(s" - Sum of error delta: ${sumErrorDeltaArr.mkString(", ")}")
        DBTFDriver.log(s" - Min. sum of error delta: ${sumErrorDelta.get}")
      } else { // iter >= 2
        val t0Core = System.nanoTime()
        DBTFDriver.log(s"Updating core tensor...")
        val (updatedCoreTensor, numCoreTensorBitUpdates) = updateCoreTensor(sc, coreTensor, ranks, partInputTensorRDD,
          brPartitionRange, A, dimA, B, dimB, C, dimC)
        DBTFDriver.log(s"Core tensor updated.")
        val timeCore = System.nanoTime() - t0Core
        iterTimeCore += timeCore
        totalTimeCore += timeCore

        val t0Factor = System.nanoTime()
        val (updatedA, updatedB, updatedC, _numFactorMatrixBitUpdates, _sumErrorDelta) = updateAllFactorMatricesTucker(
          sc, updatedCoreTensor, A, (i, ranks._1), B, (j, ranks._2), C, (k, ranks._3),
          partUnfoldedTensorMode1MapBasedRDD, partUnfoldedTensorMode2MapBasedRDD, partUnfoldedTensorMode3MapBasedRDD,
          partUnfoldedTensorMode1ArrayBasedRDD, partUnfoldedTensorMode2ArrayBasedRDD, partUnfoldedTensorMode3ArrayBasedRDD,
          partitionRange1, partitionRange2, partitionRange3, singleBitArrays, numUnfoldedTensorPartitions, numColsToUpdate,
          probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
        )
        val timeFactors = System.nanoTime() - t0Factor
        iterTimeFactors += timeFactors
        totalTimeFactors += timeFactors

        A = updatedA
        B = updatedB
        C = updatedC
        coreTensor = updatedCoreTensor
        sumErrorDelta = Some(_sumErrorDelta)

        val t1 = System.nanoTime()
        DBTFDriver.log(s"End of Iteration-$iter")
        DBTFDriver.log(s" - Elapsed time: ${(t1 - t0) / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Elapsed time (updating factors): ${iterTimeFactors / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Elapsed time (updating a core): ${iterTimeCore / math.pow(10, 9)} secs")
        DBTFDriver.log(s" - Number of bit updates in factor matrices: ${_numFactorMatrixBitUpdates}")
        DBTFDriver.log(s" - Number of bit updates in the core tensor: $numCoreTensorBitUpdates")
        DBTFDriver.log(s" - Sum of error delta: ${_sumErrorDelta}")
      }

      if (computeError == DBTFDriver.COMPUTE_ERROR_FOR_EVERY_ITER) {
        reconstructionErrors += computeReconstructionError(sc, DBTFDriver.FACTORIZATION_METHOD_TK, tensorRDD, A, B, C, dimA, dimB, dimC, Option(coreTensor))
      }

      /** Start of checking for convergence */
      convergenceMethod match {
        case Convergence.BY_ABS_ERROR_DELTA =>
          if (math.abs(sumErrorDelta.get) < convergeByAbsErrorDeltaValue) {
            numConsecutiveIters += 1
          } else {
            numConsecutiveIters = 0
          }
          if (numConsecutiveIters >= numConsecutiveItersForConvergeByAbsErrorDelta) {
            converged = true
          }
          if (convergeByNumIters && iter == convergeByNumItersValue) {
            converged = true
          }

          iter += 1
        case Convergence.BY_NUM_ITERATION =>
          if (iter == convergeByNumItersValue) {
            converged = true
          }

          iter += 1
        case Convergence.BY_NUM_BIT_UPDATES =>
          throw new IllegalArgumentException
      }
      /** End of checking for convergence */
    } while (!converged)

    partUnfoldedTensorMode1MapBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode2MapBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode3MapBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode1ArrayBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode2ArrayBasedRDD.foreach(_.unpersist(blocking = false))
    partUnfoldedTensorMode3ArrayBasedRDD.foreach(_.unpersist(blocking = false))
    partInputTensorRDD.unpersist(blocking = false)

    (A, B, C, Option(coreTensor), reconstructionErrors.toArray, totalTimeFactors, totalTimeCore)
  }

  def computeReconstructionError(sc: SparkContext, factorizationMethod: String, tensorRDD: RDD[(Int, Int, Int)],
                                 factor1: Array[(Int, Int)],
                                 factor2: Array[(Int, Int)], factor3: Array[(Int, Int)],
                                 dim1: (Int, Int), dim2: (Int, Int), dim3: (Int, Int),
                                 coreTensor: Option[Array[(Int, Int, Int)]] = None): Double = {
    log("\nComputing reconstruction error...")
    val approxTensorRDD: RDD[(Int, Int, Int)] =
      if (factorizationMethod == DBTFDriver.FACTORIZATION_METHOD_CP) {
        TensorOperation.generateTensorRDDFromFactors(sc, factor1, factor2, factor3, baseIndex = 0)
      } else {
        val approxTensor = TensorOperation.buildApproximateBooleanTensor(coreTensor.get.toSet, factor1,
          dim1, factor2, dim2, factor3, dim3, baseIndex = 0).toArray
        sc.parallelize(approxTensor)
      }
    val reconstructionError = TensorOperation.computeSumAbsDiff(sc, approxTensorRDD, tensorRDD)
    val totalElements = dim1._1 * dim2._1 * dim3._1
    val relativeReconstructionError = reconstructionError / totalElements
    log(s" - number of non-zeros: ${tensorRDD.count()}\n" +
      s" - reconstruction error: $reconstructionError\n" +
      s" - relative reconstruction error: $relativeReconstructionError ($reconstructionError / $totalElements)")
    reconstructionError
  }

  def initCoreTensor(ranks: (Int, Int, Int), initialCoreTensorDensity: Double): Array[(Int, Int, Int)] = {
    TensorOperation.generateRandomThreeWayTensor(ranks, initialCoreTensorDensity, baseIndex = 0, random = random)
  }

  def summarizeFactorRows(denseFactor: Array[Array[Byte]], coreTensor: Array[(Int, Int, Int)], mode: Int): Array[Int] = {
    assert(Array(1, 2, 3).contains(mode), mode)

    val allIndicesOfMode: Array[Int] =
      mode match {
        case 1 => Set(coreTensor.map(_._1): _*).toArray
        case 2 => Set(coreTensor.map(_._2): _*).toArray
        case 3 => Set(coreTensor.map(_._3): _*).toArray
      }

    denseFactor.map(row => allIndicesOfMode.map(i => row(i).toInt).sum)
  }

  def updateCoreTensor(sc: SparkContext, coreTensorArray: Array[(Int, Int, Int)], coreTensorDim: (Int, Int, Int),
                       partInputTensorRDD: RDD[(Int, TensorEntrySet)],
                       brPartitionRange: Broadcast[InputTensorPartitionRange],
                       sparseA: Array[(Int, Int)], dimA: (Int, Int), sparseB: Array[(Int, Int)], dimB: (Int, Int),
                       sparseC: Array[(Int, Int)], dimC: (Int, Int)): (Array[(Int, Int, Int)], Long) = {
    val coreTensor: mutable.Set[(Int, Int, Int)] = mutable.Set(coreTensorArray: _*)
    val denseA: Array[Array[Byte]] = MatrixOperation.sparseToDenseMatrix(sparseA, dimA)
    val denseB: Array[Array[Byte]] = MatrixOperation.sparseToDenseMatrix(sparseB, dimB)
    val denseC: Array[Array[Byte]] = MatrixOperation.sparseToDenseMatrix(sparseC, dimC)
    val (brDenseA, brDenseB, brDenseC) = (sc.broadcast(denseA), sc.broadcast(denseB), sc.broadcast(denseC))

    val numTotalPartitions = brPartitionRange.value.numInputTensorPartitions
    val colwiseSparseAIndexesMap = new Array[Array[Array[Int]]](numTotalPartitions)
    val colwiseSparseBIndexesMap = new Array[Array[Array[Int]]](numTotalPartitions)
    val colwiseSparseCIndexesMap = new Array[Array[Array[Int]]](numTotalPartitions)

    val usedPartitionIds = partInputTensorRDD.keys.collect()
    usedPartitionIds.foreach {
      partitionId =>
        val (mode1Range, mode2Range, mode3Range) = brPartitionRange.value.getPartitionInfo(partitionId)
        colwiseSparseAIndexesMap(partitionId) = getColumnwiseSparseNonZeroIndexesWithinRange(sparseA, dimA, mode1Range)
        colwiseSparseBIndexesMap(partitionId) = getColumnwiseSparseNonZeroIndexesWithinRange(sparseB, dimB, mode2Range)
        colwiseSparseCIndexesMap(partitionId) = getColumnwiseSparseNonZeroIndexesWithinRange(sparseC, dimC, mode3Range)
    }
    val (brColwiseSparseAIndexesMap, brColwiseSparseBIndexesMap, brColwiseSparseCIndexesMap) =
      (sc.broadcast(colwiseSparseAIndexesMap), sc.broadcast(colwiseSparseBIndexesMap), sc.broadcast(colwiseSparseCIndexesMap))

    var lastUpdate = 0 // 0: no change, 1: changed from 0 to 1, -1: changed from 1 to 0
    var numUpdates = 0L
    var curIter = -1L
    var prevR2 = -1
    var (brMode1RowSummary, brMode2RowSummary, brMode3RowSummary) =
      (sc.broadcast(summarizeFactorRows(denseA, coreTensorArray, 1)), sc.broadcast(summarizeFactorRows(denseB, coreTensorArray, 2)), sc.broadcast(summarizeFactorRows(denseC, coreTensorArray, 3)))

    for (r1 <- 0 until coreTensorDim._1; r2 <- 0 until coreTensorDim._2; r3 <- 0 until coreTensorDim._3) {
      curIter += 1
      val coreTensorIndex = (r1, r2, r3)
      if (prevR2 != r2)
        DBTFDriver.log(s"\tUpdating ($r1, $r2, $r3) at ${new SimpleDateFormat("k:mm:ss.SSS").format(Calendar.getInstance.getTime)}...")

      val coreTensorIndexExists = coreTensor.contains(coreTensorIndex)
      val brCoreTensor: Broadcast[mutable.Set[(Int, Int, Int)]] = sc.broadcast(coreTensor)
      val gain = sc.longAccumulator("Gain Accumulator")

      if (lastUpdate != 0) {
        val _coreTensorArray = coreTensor.toArray
        brMode1RowSummary = sc.broadcast(summarizeFactorRows(denseA, _coreTensorArray, 1))
        brMode2RowSummary = sc.broadcast(summarizeFactorRows(denseB, _coreTensorArray, 2))
        brMode3RowSummary = sc.broadcast(summarizeFactorRows(denseC, _coreTensorArray, 3))
      }

      partInputTensorRDD.foreachPartition {
        partIterator =>
          var iterCount = 0
          // val coreTensor: Array[(Int, Int, Int)] = brCoreTensor.value.toArray
          val (colwiseSparseAIndexesMap, colwiseSparseBIndexesMap, colwiseSparseCIndexesMap) = (brColwiseSparseAIndexesMap.value, brColwiseSparseBIndexesMap.value, brColwiseSparseCIndexesMap.value)
          // val (denseA, denseB, denseC) = (brDenseA.value, brDenseB.value, brDenseC.value)
          val (mode1RowSummary, mode2RowSummary, mode3RowSummary) = (brMode1RowSummary.value, brMode2RowSummary.value, brMode3RowSummary.value)
          val outer = new Breaks

          outer.breakable {
            partIterator foreach {
              case (partitionId: Int, inputTensorEntrySet: TensorEntrySet) =>
                val (colwiseSparseAIndexes, colwiseSparseBIndexes, colwiseSparseCIndexes) = (colwiseSparseAIndexesMap(partitionId), colwiseSparseBIndexesMap(partitionId), colwiseSparseCIndexesMap(partitionId))
                val (forI, forJ, forK) = (new Breaks, new Breaks, new Breaks)

                if (!coreTensorIndexExists) {
                  var (noApproxTensorEntryExistsI, noApproxTensorEntryExistsIJ, noApproxTensorEntryExistsIJK) = (false, false, false)
                  for (i <- colwiseSparseAIndexes(r1)) {
                    noApproxTensorEntryExistsI = mode1RowSummary(i) == 0

                    for (j <- colwiseSparseBIndexes(r2)) {
                      noApproxTensorEntryExistsIJ = noApproxTensorEntryExistsI || (mode2RowSummary(j) == 0)

                      for (k <- colwiseSparseCIndexes(r3)) {
                        noApproxTensorEntryExistsIJK = noApproxTensorEntryExistsIJ || (mode3RowSummary(k) == 0)

                         if (noApproxTensorEntryExistsIJK && inputTensorEntrySet.contains((i, j, k))) {
                          gain.add(1)
                          outer.break
                        }
                      } // for K
                    } // for J
                  } // for I
                } else { // coreTensorIndexExists
                  for (i <- colwiseSparseAIndexes(r1)) {
                    forI.breakable {
                      if (mode1RowSummary(i) != 1) forI.break

                      for (j <- colwiseSparseBIndexes(r2)) {
                        forJ.breakable {
                          if (mode2RowSummary(j) != 1) forJ.break

                          for (k <- colwiseSparseCIndexes(r3)) {
                            forK.breakable {
                              if (mode3RowSummary(k) != 1) forK.break
                              val inputTensorIndex = (i, j, k)

                              if (!inputTensorEntrySet.contains(inputTensorIndex)) {
                                // assert(approximateTensorHasEntryAtIndex(inputTensorIndex, coreTensor, denseA, denseB, denseC))
                                gain.add(1)
                                outer.break
                              }
                            }
                          } // for K
                        }
                      } // for J
                    }
                  } // for I
                }
                iterCount += 1
            } // end of partIterator foreach
          }
          assert(iterCount >= 0, s"iterCount=$iterCount")
      } // end of partInputTensorRDD.foreachPartition

      brCoreTensor.unpersist(blocking = false)

      // flip an element if gain is positive.
      lastUpdate = 0
      if (gain.value > 0) {
        numUpdates += 1
        if (coreTensor.contains(coreTensorIndex)) {
          val ret = coreTensor.remove(coreTensorIndex)
          assert(ret)
          DBTFDriver.log(s"\t- Removed a core tensor element at $coreTensorIndex.")
          lastUpdate = -1
        } else {
          val ret = coreTensor.add(coreTensorIndex)
          assert(ret)
          DBTFDriver.log(s"\t- Added a core tensor element at $coreTensorIndex.")
          lastUpdate = 1
        }
      }
      gain.reset()
      prevR2 = r2
    } // end of for (r1 <- 0 until R1; r2 <- 0 until R2; r3 <- 0 until R3)
    Array(brDenseA, brDenseB, brDenseC, brColwiseSparseAIndexesMap, brColwiseSparseBIndexesMap, brColwiseSparseCIndexesMap).foreach(_.unpersist(blocking = false))

    DBTFDriver.log(s"\tNumber of nonzeros in core tensor: ${coreTensor.toArray.length} (${coreTensor.toArray.length / (coreTensorDim._1 * coreTensorDim._2 * coreTensorDim._3).toDouble})")

    (coreTensor.toArray, numUpdates)
  }

  def getColumnwiseSparseNonZeroIndexesWithinRange(sparseMatrix: Array[(Int, Int)], matrixDim: (Int, Int),
                                                   rowRange: (Int, Int), baseIndex: Int = 0): Array[Array[Int]] = {
    assert(rowRange._1 >= 0 && rowRange._1 <= rowRange._2 && rowRange._2 < matrixDim._1, (rowRange, matrixDim))

    val (_, numCols) = matrixDim
    val columnwiseSparseNonZeroIndexes = new Array[mutable.ArrayBuffer[Int]](numCols)
    for (i <- columnwiseSparseNonZeroIndexes.indices) {
      columnwiseSparseNonZeroIndexes(i) = mutable.ArrayBuffer[Int]()
    }
    val columnwiseSparseMat = MatrixOperation.sparseToColumnwiseSparseMatrix(sparseMatrix, matrixDim._2, baseIndex)

    for (c <- 0 until numCols) {
      val sparseCol = columnwiseSparseMat(c).filter {
        r =>
          val r0 = r - baseIndex
          r0 >= rowRange._1 && r0 <= rowRange._2
      }

      columnwiseSparseNonZeroIndexes(c).appendAll(sparseCol)
    }

    columnwiseSparseNonZeroIndexes.map(_.toArray)
  }

  def approximateTensorHasEntryAtIndex(inputTensorIndex: (Int, Int, Int), coreTensor: Array[(Int, Int, Int)],
                                       A: Array[Array[Byte]], B: Array[Array[Byte]], C: Array[Array[Byte]]): Boolean = {
    val (i0, j0, k0) = inputTensorIndex

    var value = false
    breakable {
      coreTensor.foreach {
        case (r1, r2, r3) =>
          if (A(i0)(r1) == 1 && B(j0)(r2) == 1 && C(k0)(r3) == 1) {
            value = true
            break
          }
      }
    }

    value
  }

  /**
    * Update factor matrices A, B, and C alternately, and return update factor matrices
    * along with the number of bit updates and the sum of error delta.
    *
    * @param sc spark context
    * @param coreTensor three-way core tensor
    * @param initA initial factor matrix A
    * @param dimA row and column dimensions of matrix A
    * @param initB initial factor matrix B
    * @param dimB row and column dimensions of matrix B
    * @param initC initial factor matrix C
    * @param dimC row and column dimensions of matrix C
    * @param partUnfoldedTensorMode1MapBasedRDD RDD of map-based, partitioned unfolded tensor for mode 1
    * @param partUnfoldedTensorMode2MapBasedRDD RDD of map-based, partitioned unfolded tensor for mode 2
    * @param partUnfoldedTensorMode3MapBasedRDD RDD of map-based, partitioned unfolded tensor for mode 3
    * @param partUnfoldedTensorMode1ArrayBasedRDD RDD of array-based, partitioned unfolded tensor for mode 1
    * @param partUnfoldedTensorMode2ArrayBasedRDD RDD of array-based, partitioned unfolded tensor for mode 2
    * @param partUnfoldedTensorMode3ArrayBasedRDD RDD of array-based, partitioned unfolded tensor for mode 3
    * @param partitionRange1 instance for partitioning range of mode 1
    * @param partitionRange2 instance for partitioning range of mode 2
    * @param partitionRange3 instance for partitioning range of mode 3
    * @param singleBitArrays array of single bit array
    * @param numUnfoldedTensorPartitions number of partitions of unfolded tensors
    * @param numColsToUpdate number of columns to update
    * @param probForZeroForTieBreaking probability to choose zero when several column values (including zero) have the same error delta
    * @param maxZeroPercentage maximum percentage of zeros in a factor matrix
    * @param tablesOfNumZeros table mapping a value to the number of zeros in it
    * @param maxRankSplitSize maximum number of rows out of which to build a single cache table
    * @return (update factor matrix A, update factor matrix B, update factor matrix C, number of bit updates, sum of error delta)
    */
  def updateAllFactorMatricesTucker(sc: SparkContext,
                                    coreTensor: Array[(Int, Int, Int)],
                                    initA: Array[(Int, Int)], dimA: (Int, Int),
                                    initB: Array[(Int, Int)], dimB: (Int, Int),
                                    initC: Array[(Int, Int)], dimC: (Int, Int),
                                    partUnfoldedTensorMode1MapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                                    partUnfoldedTensorMode2MapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                                    partUnfoldedTensorMode3MapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                                    partUnfoldedTensorMode1ArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                                    partUnfoldedTensorMode2ArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                                    partUnfoldedTensorMode3ArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                                    partitionRange1: UnfoldedTensorPartitionRange,
                                    partitionRange2: UnfoldedTensorPartitionRange,
                                    partitionRange3: UnfoldedTensorPartitionRange,
                                    singleBitArrays: Array[Array[JBitSet]],
                                    numUnfoldedTensorPartitions: Int,
                                    numColsToUpdate: Int,
                                    probForZeroForTieBreaking: Double,
                                    maxZeroPercentage: Double,
                                    tablesOfNumZeros: Array[Array[Int]],
                                    maxRankSplitSize: Int): (Array[(Int, Int)], Array[(Int, Int)], Array[(Int, Int)], Long, Long) = {
    var A = initA
    var B = initB
    var C = initC

    /** Unfold core tensor into three modes */
    val coreTensorModeLengths = (dimA._2, dimB._2, dimC._2)
    val unfoldedCoreMode1: Array[(Int, Int)] = TensorOperation.unfoldTensor(coreTensor, coreTensorModeLengths)(mode = 1).map(e => (e._1.toInt, e._2.toInt))
    val unfoldedCoreMode2: Array[(Int, Int)] = TensorOperation.unfoldTensor(coreTensor, coreTensorModeLengths)(mode = 2).map(e => (e._1.toInt, e._2.toInt))
    val unfoldedCoreMode3: Array[(Int, Int)] = TensorOperation.unfoldTensor(coreTensor, coreTensorModeLengths)(mode = 3).map(e => (e._1.toInt, e._2.toInt))
    val rowwiseUnfoldedCoreMode1 = MatrixOperation.rowwiseMatrix(unfoldedCoreMode1, coreTensorModeLengths._1)
    val rowwiseUnfoldedCoreMode2 = MatrixOperation.rowwiseMatrix(unfoldedCoreMode2, coreTensorModeLengths._2)
    val rowwiseUnfoldedCoreMode3 = MatrixOperation.rowwiseMatrix(unfoldedCoreMode3, coreTensorModeLengths._3)
    val brAllPairsRowSummationUnfoldedCoreMode1 = sc.broadcast(computeRowSummationForAllPairs(rowwiseUnfoldedCoreMode1, maxRankSplitSize))
    val brAllPairsRowSummationUnfoldedCoreMode2 = sc.broadcast(computeRowSummationForAllPairs(rowwiseUnfoldedCoreMode2, maxRankSplitSize))
    val brAllPairsRowSummationUnfoldedCoreMode3 = sc.broadcast(computeRowSummationForAllPairs(rowwiseUnfoldedCoreMode3, maxRankSplitSize))

    /** Update factor matrix A */
    DBTFDriver.log(s"Updating factor matrix A...")
    val brBitRowMatrixC: Broadcast[Array[JBitSet]] = sc.broadcast(MatrixOperation.sparseToBitRowMatrix(C, dimC, singleBitArrays(2)))
    val rowwiseMatrixB: Array[Array[Int]] = MatrixOperation.rowwiseMatrix(MatrixOperation.transpose(B), dimB._2)
    val brAllPairsRowSummationB = sc.broadcast(computeRowSummationForAllPairs(rowwiseMatrixB, maxRankSplitSize))
    val (newA, numBitUpdates1, sumErrorDelta1) = updateFactorMatrixTucker(
      sc, numUnfoldedTensorPartitions, partUnfoldedTensorMode1MapBasedRDD, partUnfoldedTensorMode1ArrayBasedRDD, A, dimA, brBitRowMatrixC, dimC, brAllPairsRowSummationB,
      dimB, brAllPairsRowSummationUnfoldedCoreMode1, singleBitArrays(0), numColsToUpdate, probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
    )
    A = newA
    brAllPairsRowSummationB.unpersist(blocking = false)

    /** Update factor matrix B */
    DBTFDriver.log(s"Updating factor matrix B...")
    val rowwiseMatrixA: Array[Array[Int]] = MatrixOperation.rowwiseMatrix(MatrixOperation.transpose(A), dimA._2)
    val brAllPairsRowSummationA = sc.broadcast(computeRowSummationForAllPairs(rowwiseMatrixA, maxRankSplitSize))
    val (newB, numBitUpdates2, sumErrorDelta2) = updateFactorMatrixTucker(
      sc, numUnfoldedTensorPartitions, partUnfoldedTensorMode2MapBasedRDD, partUnfoldedTensorMode2ArrayBasedRDD, B, dimB, brBitRowMatrixC, dimC, brAllPairsRowSummationA,
      dimA, brAllPairsRowSummationUnfoldedCoreMode2, singleBitArrays(1), numColsToUpdate, probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
    )
    B = newB

    /** Update factor matrix C */
    DBTFDriver.log(s"Updating factor matrix C...")
    val brBitRowMatrixB: Broadcast[Array[JBitSet]] = sc.broadcast(MatrixOperation.sparseToBitRowMatrix(B, dimB, singleBitArrays(1)))
    val (newC, numBitUpdates3, sumErrorDelta3) = updateFactorMatrixTucker(
      sc, numUnfoldedTensorPartitions, partUnfoldedTensorMode3MapBasedRDD, partUnfoldedTensorMode3ArrayBasedRDD, C, dimC, brBitRowMatrixB, dimB, brAllPairsRowSummationA,
      dimA, brAllPairsRowSummationUnfoldedCoreMode3, singleBitArrays(2), numColsToUpdate, probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
    )
    C = newC

    brAllPairsRowSummationA.unpersist(blocking = false)
    brBitRowMatrixB.unpersist(blocking = false)
    brBitRowMatrixC.unpersist(blocking = false)

    val numBitUpdates: Long = numBitUpdates1 + numBitUpdates2 + numBitUpdates3
    val sumErrorDelta: Long = sumErrorDelta1 + sumErrorDelta2 + sumErrorDelta3

    (A, B, C, numBitUpdates, sumErrorDelta)
  }

  /**
    * Update the given factor matrix, and return the updated target factor matrix
    * along with the number of bit updates, and the sum of error delta
    *
    * @param sc spark context
    * @param numUnfoldedTensorPartitions number of partitions of unfolded tensors
    * @param _partUnfoldedTensorMapBasedRDD RDD of map-based, partitioned unfolded tensor
    * @param _partUnfoldedTensorArrayBasedRDD RDD of array-based, partitioned unfolded tensor
    * @param initTargetFactorMatrix initial target factor matrix
    * @param targetFactorMatrixDim row and column dimensions of a target factor matrix
    * @param brBitRowFactorMatrix1 broadcast variable for bit row factor matrix1
    * @param factorMatrix1Dim row and column dimensions of the first factor matrix in the Kronecker product
    * @param brAllPairsRowSummations broadcast variable for all pairs of row summations
    * @param factorMatrix2Dim row and column dimensions of the second factor matrix in the Kronecker product
    * @param brAllPairsRowSummationUnfoldedCore broadcast variable for all pairs of row summations of unfolded core tensor
    * @param singleBitArray single bit array
    * @param numColsToUpdate number of columns to update
    * @param probForZeroForTieBreaking probability to choose zero when several column values (including zero) have the same error delta
    * @param maxZeroPercentage maximum percentage of zeros in a factor matrix
    * @param tablesOfNumZeros table mapping a value to the number of zeros in it
    * @param maxRankSplitSize maximum number of rows out of which to build a single cache table
    * @return (updated target factor matrix, number of bit updates, sum of error delta)
    */
  def updateFactorMatrixTucker(sc: SparkContext,
                               numUnfoldedTensorPartitions: Int,
                               _partUnfoldedTensorMapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                               _partUnfoldedTensorArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                               initTargetFactorMatrix: Array[(Int, Int)],
                               targetFactorMatrixDim: (Int, Int),
                               brBitRowFactorMatrix1: Broadcast[Array[JBitSet]],
                               factorMatrix1Dim: (Int, Int),
                               brAllPairsRowSummations: Broadcast[Array[Array[Array[Int]]]],
                               factorMatrix2Dim: (Int, Int),
                               brAllPairsRowSummationUnfoldedCore: Broadcast[Array[Array[Array[Int]]]],
                               singleBitArray: Array[JBitSet],
                               numColsToUpdate: Int,
                               probForZeroForTieBreaking: Double,
                               maxZeroPercentage: Double,
                               tablesOfNumZeros: Array[Array[Int]],
                               maxRankSplitSize: Int): (Array[(Int, Int)], Long, Long) = {
    val (targetFactorMatrixRowLength, targetFactorMatrixRank) = targetFactorMatrixDim
    val (_, factorMatrix1Rank) = factorMatrix1Dim
    val (_, factorMatrix2Rank) = factorMatrix2Dim
    val newColumnsVector: Array[Int] = Array.fill[Int](targetFactorMatrixRowLength)(-1)
    val errorDeltaVector: Array[Long] = Array.fill[Long](targetFactorMatrixRowLength)(0)
    val targetBitRowFactorMatrix: Array[JBitSet] = MatrixOperation.sparseToBitRowMatrix(initTargetFactorMatrix, targetFactorMatrixDim, singleBitArray)
    if (DEBUG) {
      DBTFDriver.log("targetBitRowFactorMatrix:")
      targetBitRowFactorMatrix.foreach(bitRow => DBTFDriver.log(bitSetToBinaryString(bitRow, targetFactorMatrixRank)))
    }
    var numBitUpdates = 0L
    var sumErrorDelta = 0L

    // build summationMapRDD that contains summation map(s) necessary for each partition
    val summationMapRDD = buildSummationMapForRowwiseMatrix(_partUnfoldedTensorMapBasedRDD, _partUnfoldedTensorArrayBasedRDD, brAllPairsRowSummations, factorMatrix2Dim._1)
    summationMapRDD.cache()

    assert({ // partition ids should be unique
      val partUnfoldedTensorRDD = if (_partUnfoldedTensorMapBasedRDD.isDefined) _partUnfoldedTensorMapBasedRDD.get else _partUnfoldedTensorArrayBasedRDD.get
      val partitionIds = partUnfoldedTensorRDD.map(_._1).collect()
      partitionIds.distinct.length == partitionIds.length
    })
    val targetFactorRangeSplitSizes: Array[Int] = splitRangeWithMaxSplitSize(0, targetFactorMatrixRank - 1, maxRankSplitSize).map(t => t._2 - t._1 + 1)
    val factorMatrix2RangeSplitSizes: Array[Int] = splitRangeWithMaxSplitSize(0, factorMatrix2Rank - 1, maxRankSplitSize).map(t => t._2 - t._1 + 1)

    /** START of column-wise update loop **/
    for (colsStartIndex0 <- 0 until targetFactorMatrixRank by numColsToUpdate) {
      val colsEndIndex0 = math.min(colsStartIndex0 + numColsToUpdate - 1, targetFactorMatrixRank - 1)
      val bitRowMasks = generateBitRowMasks(colsStartIndex0, colsEndIndex0, targetFactorMatrixRank)

      DBTFDriver.log(s"\tUpdating columns from column-$colsStartIndex0 to column-$colsEndIndex0...")

      /** START of RDD.mapPartitions **/
      // Note: partitionedErrorVectorRDD does not need to be cached. It is collected only once in updateFactorMatrixColumns.
      val partitionedErrorVectorRDD: RDD[((Int, Int), Array[Array[Int]])] =
        if (_partUnfoldedTensorMapBasedRDD.isDefined) {
          _partUnfoldedTensorMapBasedRDD.get.join(summationMapRDD) mapPartitionsWithIndex { (partitionIndex: Int, partIterator) =>
            val bitRowFactorMatrix1: Array[JBitSet] = brBitRowFactorMatrix1.value
            val allPairsRowSummationUnfoldedCore: Array[Array[Array[Int]]] = brAllPairsRowSummationUnfoldedCore.value
            val errorVectors = MatrixOperation.initTwoDimArray(bitRowMasks.length, targetFactorMatrixRowLength)

            var iterCount = 0
            partIterator.foreach {
              case (partitionId: Int, (groupedPartRowwiseMap: Array[((Int, (Int, Int)), Map[Long, Array[Int]])],
              summationMapForRowwiseMatrix2: mutable.HashMap[(Int, Int), Array[Array[Array[Int]]]])) =>
                assert(partitionId == partitionIndex, s"partitionId: $partitionId, partitionIndex: $partitionIndex")

                groupedPartRowwiseMap foreach {
                  case ((factorMatrix1BitArrayRowIndex0: Int, (rowSummationHashTableRangeStart: Int, rowSummationHashTableRangeEnd: Int)),
                  partRowwiseMap: Map[Long, Array[Int]]) =>
                    for (rowIndex0 <- 0 until targetFactorMatrixRowLength) {
                      val unfoldedTensorRowPiece = partRowwiseMap.getOrElse(rowIndex0, null)

                      /** START of error computation */
                      assert(unfoldedTensorRowPiece == null || unfoldedTensorRowPiece.forall(x => x < factorMatrix2Dim._1)) // check the range of elements in unfoldedTensorRowPiece
                      val targetFactorBitRow = targetBitRowFactorMatrix(rowIndex0)

                      for (tryVal <- bitRowMasks.indices) {
                        val tryTargetFactorBitRow = setBits(targetFactorBitRow.clone().asInstanceOf[JBitSet], targetFactorMatrixRank, colsStartIndex0, colsEndIndex0, tryVal, bitRowMasks)._1
                        val unfoldedCoreRowSummation: Array[Int] = constructTensorRow(tryTargetFactorBitRow, allPairsRowSummationUnfoldedCore, targetFactorMatrixRank, targetFactorRangeSplitSizes, maxRankSplitSize)
                        val factorMatrix1BitRow = bitRowFactorMatrix1(factorMatrix1BitArrayRowIndex0)
                        val cacheKey: JBitSet = computeCacheKeyForTucker(unfoldedCoreRowSummation, factorMatrix1BitRow, factorMatrix1Rank, factorMatrix2Rank)

                        val rowSummationsArray: Array[Array[Array[Int]]] = summationMapForRowwiseMatrix2((rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd))
                        val constructedTensorRowPiece = constructTensorRow(cacheKey, rowSummationsArray, factorMatrix2Rank, factorMatrix2RangeSplitSizes, maxRankSplitSize)
                        // compute the difference between the constructed tensor row and the corresponding row piece of unfolded input tensor
                        val distance = computeTensorRowDistance(constructedTensorRowPiece, unfoldedTensorRowPiece)
                        errorVectors(tryVal)(rowIndex0) += distance
                      }
                      /** END of error computation */
                    }
                }
                iterCount += 1
            }
            if (iterCount == 0) {
              throw new Exception(s"ERROR: partitionIndex=$partitionIndex is empty. Try again with a smaller value for the '--num-unfolded-tensor-partitions' parameter.")
            }
            assert(iterCount == 1, s"partitionIndex: $partitionIndex, iterCount: $iterCount") // assert an assumption that only one (logical) partition is sent to each task.

            partitionErrorVectors(errorVectors, numUnfoldedTensorPartitions).toIterator
          }
        } else {
          _partUnfoldedTensorArrayBasedRDD.get.join(summationMapRDD) mapPartitionsWithIndex { (partitionIndex: Int, partIterator) =>
            val bitRowFactorMatrix1: Array[JBitSet] = brBitRowFactorMatrix1.value
            val allPairsRowSummationUnfoldedCore: Array[Array[Array[Int]]] = brAllPairsRowSummationUnfoldedCore.value
            val errorVectors = MatrixOperation.initTwoDimArray(bitRowMasks.length, targetFactorMatrixRowLength)

            var iterCount = 0
            partIterator.foreach {
              case (partitionId: Int, (groupedPartRowwiseMap: Array[((Int, (Int, Int)), Array[Array[Int]])],
              summationMapForRowwiseMatrix2: mutable.HashMap[(Int, Int), Array[Array[Array[Int]]]])) =>
                assert(partitionId == partitionIndex, s"partitionId: $partitionId, partitionIndex: $partitionIndex")

                groupedPartRowwiseMap foreach {
                  case ((factorMatrix1BitArrayRowIndex0: Int, (rowSummationHashTableRangeStart: Int, rowSummationHashTableRangeEnd: Int)),
                  partRowwiseArray: Array[Array[Int]]) =>
                    for (rowIndex0 <- 0 until targetFactorMatrixRowLength) {
                      val unfoldedTensorRowPiece = partRowwiseArray(rowIndex0)

                      /** START of error computation */
                      assert(unfoldedTensorRowPiece == null || unfoldedTensorRowPiece.forall(x => x < factorMatrix2Dim._1)) // check the range of elements in unfoldedTensorRowPiece
                      val targetFactorBitRow = targetBitRowFactorMatrix(rowIndex0)

                      for (tryVal <- bitRowMasks.indices) {
                        val tryTargetFactorBitRow = setBits(targetFactorBitRow.clone().asInstanceOf[JBitSet], targetFactorMatrixRank, colsStartIndex0, colsEndIndex0, tryVal, bitRowMasks)._1
                        val unfoldedCoreRowSummation: Array[Int] = constructTensorRow(tryTargetFactorBitRow, allPairsRowSummationUnfoldedCore, targetFactorMatrixRank, targetFactorRangeSplitSizes, maxRankSplitSize)
                        val factorMatrix1BitRow = bitRowFactorMatrix1(factorMatrix1BitArrayRowIndex0)
                        val cacheKey: JBitSet = computeCacheKeyForTucker(unfoldedCoreRowSummation, factorMatrix1BitRow, factorMatrix1Rank, factorMatrix2Rank)

                        val rowSummationsArray: Array[Array[Array[Int]]] = summationMapForRowwiseMatrix2((rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd))
                        val constructedTensorRowPiece = constructTensorRow(cacheKey, rowSummationsArray, factorMatrix2Rank, factorMatrix2RangeSplitSizes, maxRankSplitSize)
                        // compute the difference between the constructed tensor row and the corresponding row piece of unfolded input tensor
                        val distance = computeTensorRowDistance(constructedTensorRowPiece, unfoldedTensorRowPiece)
                        errorVectors(tryVal)(rowIndex0) += distance
                      }
                      /** END of error computation */
                    }
                }
                iterCount += 1
            }
            if (iterCount == 0) {
              throw new Exception(s"ERROR: partitionIndex=$partitionIndex is empty. Try again with a smaller value for the '--num-unfolded-tensor-partitions' parameter.")
            }
            assert(iterCount == 1, s"partitionIndex: $partitionIndex, iterCount: $iterCount") // assert an assumption that only one (logical) partition is sent to each task.

            partitionErrorVectors(errorVectors, numUnfoldedTensorPartitions).toIterator
          }
        }
      /** END of RDD.mapPartitions **/
      val targetBitRowFactorMatrixString: Option[Array[String]] =
        if (DEBUG) {
          Some(targetBitRowFactorMatrix.map { bitRow => bitSetToBinaryString(bitRow, targetFactorMatrixRank) })
        } else {
          None
        }

      updateFactorMatrixColumns(partitionedErrorVectorRDD, newColumnsVector, targetBitRowFactorMatrix, targetFactorMatrixRank,
        colsStartIndex0, colsEndIndex0, errorDeltaVector, tablesOfNumZeros, probForZeroForTieBreaking, maxZeroPercentage)
      val _sumErrorDelta = errorDeltaVector.sum

      // update target factor matrix with the new column vector.
      val _numBitUpdates = updateTargetBitRowFactorMatrix(targetBitRowFactorMatrix, targetFactorMatrixRank, newColumnsVector, colsStartIndex0, colsEndIndex0, bitRowMasks)

      if (DEBUG) {
        targetBitRowFactorMatrixString.zip(targetBitRowFactorMatrix) foreach {
          case (beforeStr, after) => DBTFDriver.log(s"$beforeStr -> ${bitSetToBinaryString(after, targetFactorMatrixRank)}")
        }
      }

      sumErrorDelta += _sumErrorDelta
      numBitUpdates += _numBitUpdates
    }
    /** END of column-wise update loop */

    summationMapRDD.unpersist(blocking = false)

    val targetFactorMatrix: Array[(Int, Int)] = MatrixOperation.bitRowToSparseMatrix(targetBitRowFactorMatrix, targetFactorMatrixRank)
    DBTFDriver.log(s"\tNumber of nonzeros: ${targetFactorMatrix.length} (${targetFactorMatrix.length / (targetFactorMatrixDim._1 * targetFactorMatrixDim._2).toDouble})")
    (targetFactorMatrix, numBitUpdates, sumErrorDelta)
  }

  def computeCacheKeyForTucker(unfoldedCoreRowSummation: Array[Int], factorMatrix1BitRow: JBitSet, factorMatrix1Rank: Int, factorMatrix2Rank: Int): JBitSet = {
    assert(factorMatrix1Rank > 0, factorMatrix1Rank)
    assert(factorMatrix2Rank > 0, factorMatrix2Rank)
    assert(unfoldedCoreRowSummation.foldLeft(true) { case (a, v) => a && (v <= factorMatrix1Rank * factorMatrix2Rank) }, unfoldedCoreRowSummation.mkString(", "))

    val cacheKey = new JBitSet(factorMatrix2Rank)
    if (unfoldedCoreRowSummation.isEmpty) {
      return cacheKey // empty cache key
    }

    var i = 0
    val factorMatrix1Rank0 = factorMatrix1Rank - 1
    val factorMatrix2Rank0 = factorMatrix2Rank - 1
    var (rangeStartIndex0, rangeEndIndex0) = (-factorMatrix2Rank, 0)
    for (j <- factorMatrix1Rank0 to 0 by -1) {
      rangeStartIndex0 += factorMatrix2Rank // inclusive
      rangeEndIndex0 = rangeStartIndex0 + factorMatrix2Rank - 1 // inclusive

      val factorMatrix1Bit = factorMatrix1BitRow.get(j)
      if (factorMatrix1Bit) {
        val bitRow = new JBitSet()

        while (i < unfoldedCoreRowSummation.length && unfoldedCoreRowSummation(i) < rangeStartIndex0) {
          i += 1 // skipping entries that are not used.
        }

        try {
          assert(i == unfoldedCoreRowSummation.length || unfoldedCoreRowSummation(i) >= rangeStartIndex0, (unfoldedCoreRowSummation(i), rangeStartIndex0))
        } catch {
          case e: Throwable =>
            println(s"unfoldedCoreRowSummation.length=${unfoldedCoreRowSummation.length}")
            println(unfoldedCoreRowSummation.mkString(","))
            println(s"bitSetToBinaryString(factorMatrix1BitRow, factorMatrix1Rank): ${bitSetToBinaryString(factorMatrix1BitRow, factorMatrix1Rank)}")
            println(s"factorMatrix1Rank: $factorMatrix1Rank")
            println(s"factorMatrix2Rank: $factorMatrix2Rank")
            throw e
        }

        while (i < unfoldedCoreRowSummation.length && unfoldedCoreRowSummation(i) <= rangeEndIndex0 && unfoldedCoreRowSummation(i) >= rangeStartIndex0) {
          bitRow.set(factorMatrix2Rank0 - (unfoldedCoreRowSummation(i) - rangeStartIndex0))
          i += 1
        }

        cacheKey.or(bitRow)
      }
    }
    assert(i <= unfoldedCoreRowSummation.length, (i, unfoldedCoreRowSummation.length))

    cacheKey
  }

  /**
    * Update factor matrices A, B, and C alternately, and return update factor matrices
    * along with the number of bit updates and the sum of error delta.
    *
    * @param sc spark context
    * @param initA initial factor matrix A
    * @param dimA row and column dimensions of matrix A
    * @param initB initial factor matrix B
    * @param dimB row and column dimensions of matrix B
    * @param initC initial factor matrix C
    * @param dimC row and column dimensions of matrix C
    * @param partUnfoldedTensorMode1MapBasedRDD RDD of map-based, partitioned unfolded tensor for mode 1
    * @param partUnfoldedTensorMode2MapBasedRDD RDD of map-based, partitioned unfolded tensor for mode 2
    * @param partUnfoldedTensorMode3MapBasedRDD RDD of map-based, partitioned unfolded tensor for mode 3
    * @param partUnfoldedTensorMode1ArrayBasedRDD RDD of array-based, partitioned unfolded tensor for mode 1
    * @param partUnfoldedTensorMode2ArrayBasedRDD RDD of array-based, partitioned unfolded tensor for mode 2
    * @param partUnfoldedTensorMode3ArrayBasedRDD RDD of array-based, partitioned unfolded tensor for mode 3
    * @param partitionRange1 instance for partitioning range of mode 1
    * @param partitionRange2 instance for partitioning range of mode 2
    * @param partitionRange3 instance for partitioning range of mode 3
    * @param rank rank
    * @param singleBitArray single bit array
    * @param numUnfoldedTensorPartitions number of partitions of unfolded tensors
    * @param numColsToUpdate number of columns to update
    * @param probForZeroForTieBreaking probability to choose zero when several column values (including zero) have the same error delta
    * @param maxZeroPercentage maximum percentage of zeros in a factor matrix
    * @param tablesOfNumZeros table mapping a value to the number of zeros in it
    * @param maxRankSplitSize maximum number of rows out of which to build a single cache table
    * @return (update factor matrix A, update factor matrix B, update factor matrix C, number of bit updates, sum of error delta)
    */
  def updateAllFactorMatricesCP(sc: SparkContext,
                                initA: Array[(Int, Int)], dimA: (Int, Int),
                                initB: Array[(Int, Int)], dimB: (Int, Int),
                                initC: Array[(Int, Int)], dimC: (Int, Int),
                                partUnfoldedTensorMode1MapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                                partUnfoldedTensorMode2MapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                                partUnfoldedTensorMode3MapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                                partUnfoldedTensorMode1ArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                                partUnfoldedTensorMode2ArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                                partUnfoldedTensorMode3ArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                                partitionRange1: UnfoldedTensorPartitionRange,
                                partitionRange2: UnfoldedTensorPartitionRange,
                                partitionRange3: UnfoldedTensorPartitionRange,
                                rank: Int,
                                singleBitArray: Array[JBitSet],
                                numUnfoldedTensorPartitions: Int,
                                numColsToUpdate: Int,
                                probForZeroForTieBreaking: Double,
                                maxZeroPercentage: Double,
                                tablesOfNumZeros: Array[Array[Int]],
                                maxRankSplitSize: Int): (Array[(Int, Int)], Array[(Int, Int)], Array[(Int, Int)], Long, Long) = {
    var A = initA
    var B = initB
    var C = initC

    /** Update factor matrix A */
    DBTFDriver.log(s"Updating factor matrix A...")
    val brBitRowMatrixC: Broadcast[Array[JBitSet]] = sc.broadcast(MatrixOperation.sparseToBitRowMatrix(C, matrixDimension = dimC, singleBitArray))
    val rowwiseMatrixB: Array[Array[Int]] = MatrixOperation.rowwiseMatrix(MatrixOperation.transpose(B), rank)
    val brAllPairsRowSummationB = sc.broadcast(computeRowSummationForAllPairs(rowwiseMatrixB, maxRankSplitSize))
    val (newA, numBitUpdates1, sumErrorDelta1) = updateFactorMatrixCP(
      sc, numUnfoldedTensorPartitions, partUnfoldedTensorMode1MapBasedRDD, partUnfoldedTensorMode1ArrayBasedRDD, A, dimA, brBitRowMatrixC, brAllPairsRowSummationB,
      partitionRange1.matrix2RowLength, singleBitArray, numColsToUpdate, probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
    )
    A = newA
    brAllPairsRowSummationB.unpersist(blocking = false)

    /** Update factor matrix B */
    DBTFDriver.log(s"Updating factor matrix B...")
    val rowwiseMatrixA: Array[Array[Int]] = MatrixOperation.rowwiseMatrix(MatrixOperation.transpose(A), rank)
    val brAllPairsRowSummationA = sc.broadcast(computeRowSummationForAllPairs(rowwiseMatrixA, maxRankSplitSize))
    val (newB, numBitUpdates2, sumErrorDelta2) = updateFactorMatrixCP(
      sc, numUnfoldedTensorPartitions, partUnfoldedTensorMode2MapBasedRDD, partUnfoldedTensorMode2ArrayBasedRDD, B, dimB, brBitRowMatrixC, brAllPairsRowSummationA,
      partitionRange2.matrix2RowLength, singleBitArray, numColsToUpdate, probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
    )
    B = newB

    /** Update factor matrix C */
    DBTFDriver.log(s"Updating factor matrix C...")
    val brBitRowMatrixB: Broadcast[Array[JBitSet]] = sc.broadcast(MatrixOperation.sparseToBitRowMatrix(B, matrixDimension = dimB, singleBitArray))
    val (newC, numBitUpdates3, sumErrorDelta3) = updateFactorMatrixCP(
      sc, numUnfoldedTensorPartitions, partUnfoldedTensorMode3MapBasedRDD, partUnfoldedTensorMode3ArrayBasedRDD, C, dimC, brBitRowMatrixB, brAllPairsRowSummationA,
      partitionRange3.matrix2RowLength, singleBitArray, numColsToUpdate, probForZeroForTieBreaking, maxZeroPercentage, tablesOfNumZeros, maxRankSplitSize
    )
    C = newC

    brAllPairsRowSummationA.unpersist(blocking = false)
    brBitRowMatrixB.unpersist(blocking = false)
    brBitRowMatrixC.unpersist(blocking = false)

    val numBitUpdates: Long = numBitUpdates1 + numBitUpdates2 + numBitUpdates3
    val sumErrorDelta: Long = sumErrorDelta1 + sumErrorDelta2 + sumErrorDelta3

    (A, B, C, numBitUpdates, sumErrorDelta)
  }

  /**
    * Update the given factor matrix, and return the updated target factor matrix
    * along with the number of bit updates, and the sum of error delta
    *
    * @param sc spark context
    * @param numUnfoldedTensorPartitions number of partitions of unfolded tensors
    * @param _partUnfoldedTensorMapBasedRDD RDD of map-based, partitioned unfolded tensor
    * @param _partUnfoldedTensorArrayBasedRDD RDD of array-based, partitioned unfolded tensor
    * @param initTargetFactorMatrix initial target factor matrix
    * @param targetFactorMatrixDim row and column dimensions of a target factor matrix
    * @param brBitRowFactorMatrix1 broadcast variable for bit row factor matrix1
    * @param brAllPairsRowSummations broadcast variable for all pairs of row summations
    * @param matrix2RowLength row length of matrix2
    * @param singleBitArray single bit array
    * @param numColsToUpdate number of columns to update
    * @param probForZeroForTieBreaking probability to choose zero when several column values (including zero) have the same error delta
    * @param maxZeroPercentage maximum percentage of zeros in a factor matrix
    * @param tablesOfNumZeros table mapping a value to the number of zeros in it
    * @param maxRankSplitSize maximum number of rows out of which to build a single cache table
    * @return (updated target factor matrix, number of bit updates, sum of error delta)
    */
  def updateFactorMatrixCP(sc: SparkContext,
                           numUnfoldedTensorPartitions: Int,
                           _partUnfoldedTensorMapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                           _partUnfoldedTensorArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                           initTargetFactorMatrix: Array[(Int, Int)],
                           targetFactorMatrixDim: (Int, Int),
                           brBitRowFactorMatrix1: Broadcast[Array[JBitSet]],
                           brAllPairsRowSummations: Broadcast[Array[Array[Array[Int]]]],
                           matrix2RowLength: Int,
                           singleBitArray: Array[JBitSet],
                           numColsToUpdate: Int,
                           probForZeroForTieBreaking: Double,
                           maxZeroPercentage: Double,
                           tablesOfNumZeros: Array[Array[Int]],
                           maxRankSplitSize: Int): (Array[(Int, Int)], Long, Long) = {
    val (targetFactorMatrixRowLength, rank) = targetFactorMatrixDim
    val newColumnsVector: Array[Int] = Array.fill[Int](targetFactorMatrixRowLength)(-1)
    val errorDeltaVector: Array[Long] = Array.fill[Long](targetFactorMatrixRowLength)(0)
    val targetBitRowFactorMatrix: Array[JBitSet] = MatrixOperation.sparseToBitRowMatrix(initTargetFactorMatrix, targetFactorMatrixDim, singleBitArray)
    if (DEBUG) {
      DBTFDriver.log("targetBitRowFactorMatrix:")
      targetBitRowFactorMatrix.foreach(bitRow => DBTFDriver.log(bitSetToBinaryString(bitRow, rank)))
    }
    var numBitUpdates = 0L
    var sumErrorDelta = 0L

    // build summationMapRDD that contains summation map(s) necessary for each partition
    val summationMapRDD = buildSummationMapForRowwiseMatrix(_partUnfoldedTensorMapBasedRDD, _partUnfoldedTensorArrayBasedRDD, brAllPairsRowSummations, matrix2RowLength)
    summationMapRDD.cache()

    assert({ // partition ids should be unique
      val partUnfoldedTensorRDD = if (_partUnfoldedTensorMapBasedRDD.isDefined) _partUnfoldedTensorMapBasedRDD.get else _partUnfoldedTensorArrayBasedRDD.get
      val partitionIds = partUnfoldedTensorRDD.map(_._1).collect()
      partitionIds.distinct.length == partitionIds.length
    })
    val rangeSplitSizes = splitRangeWithMaxSplitSize(0, rank - 1, maxRankSplitSize).map(t => t._2 - t._1 + 1)

    /** START of column-wise update loop **/
    for (colsStartIndex0 <- 0 until rank by numColsToUpdate) {
      val colsEndIndex0 = math.min(colsStartIndex0 + numColsToUpdate - 1, rank - 1)
      val bitRowMasks = generateBitRowMasks(colsStartIndex0, colsEndIndex0, rank)

      DBTFDriver.log(s"\tUpdating columns from column-$colsStartIndex0 to column-$colsEndIndex0...")

      /** START of RDD.mapPartitions **/
      // Note: partitionedErrorVectorRDD does not need to be cached. It is collected only once in updateFactorMatrixColumns.
      val partitionedErrorVectorRDD: RDD[((Int, Int), Array[Array[Int]])] =
        if (_partUnfoldedTensorMapBasedRDD.isDefined) {
          _partUnfoldedTensorMapBasedRDD.get.join(summationMapRDD) mapPartitionsWithIndex { (partitionIndex: Int, partIterator) =>
            val bitRowFactorMatrix1: Array[JBitSet] = brBitRowFactorMatrix1.value
            val errorVectors = MatrixOperation.initTwoDimArray(bitRowMasks.length, targetFactorMatrixRowLength)

            var iterCount = 0
            partIterator.foreach {
              case (partitionId: Int, (groupedPartRowwiseMap: Array[((Int, (Int, Int)), Map[Long, Array[Int]])],
              summationMapForRowwiseMatrix2: mutable.HashMap[(Int, Int), Array[Array[Array[Int]]]])) =>
                assert(partitionId == partitionIndex, s"partitionId: $partitionId, partitionIndex: $partitionIndex")

                groupedPartRowwiseMap foreach {
                  case ((factorMatrix1BitArrayRowIndex0: Int, (rowSummationHashTableRangeStart: Int, rowSummationHashTableRangeEnd: Int)),
                  partRowwiseMap: Map[Long, Array[Int]]) =>
                    for (rowIndex0 <- 0 until targetFactorMatrixRowLength) {
                      val unfoldedTensorRowPiece = partRowwiseMap.getOrElse(rowIndex0, null)

                      /** START of error computation */
                      assert(unfoldedTensorRowPiece == null || unfoldedTensorRowPiece.forall(x => x < matrix2RowLength)) // check the range of elements in unfoldedTensorRowPiece
                      val targetFactorBitRow = targetBitRowFactorMatrix(rowIndex0)

                      for (tryVal <- bitRowMasks.indices) {
                        val tryTargetFactorBitRow = setBits(targetFactorBitRow.clone().asInstanceOf[JBitSet], rank, colsStartIndex0, colsEndIndex0, tryVal, bitRowMasks)._1
                        val factorMatrix1BitRow = bitRowFactorMatrix1(factorMatrix1BitArrayRowIndex0)
                        tryTargetFactorBitRow.and(factorMatrix1BitRow)
                        val rowSummationsArray: Array[Array[Array[Int]]] = summationMapForRowwiseMatrix2((rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd))
                        val constructedTensorRowPiece = constructTensorRow(tryTargetFactorBitRow, rowSummationsArray, rank, rangeSplitSizes, maxRankSplitSize)

                        // compute the difference between the constructed tensor row and the corresponding row piece of unfolded input tensor
                        val distance = computeTensorRowDistance(constructedTensorRowPiece, unfoldedTensorRowPiece)
                        errorVectors(tryVal)(rowIndex0) += distance
                      }
                      /** END of error computation */
                    }
                }
                iterCount += 1
            }
            if (iterCount == 0) {
              throw new Exception(s"ERROR: partitionIndex=$partitionIndex is empty. Try again with a smaller value for the '--num-unfolded-tensor-partitions' parameter.")
            }
            assert(iterCount == 1, s"partitionIndex: $partitionIndex, iterCount: $iterCount") // assert an assumption that only one (logical) partition is sent to each task.

            partitionErrorVectors(errorVectors, numUnfoldedTensorPartitions).toIterator
          }
        } else {
          _partUnfoldedTensorArrayBasedRDD.get.join(summationMapRDD) mapPartitionsWithIndex { (partitionIndex: Int, partIterator) =>
            val bitRowFactorMatrix1: Array[JBitSet] = brBitRowFactorMatrix1.value
            val errorVectors = MatrixOperation.initTwoDimArray(bitRowMasks.length, targetFactorMatrixRowLength)

            var iterCount = 0
            partIterator.foreach {
              case (partitionId: Int, (groupedPartRowwiseMap: Array[((Int, (Int, Int)), Array[Array[Int]])],
              summationMapForRowwiseMatrix2: mutable.HashMap[(Int, Int), Array[Array[Array[Int]]]])) =>
                assert(partitionId == partitionIndex, s"partitionId: $partitionId, partitionIndex: $partitionIndex")

                groupedPartRowwiseMap foreach {
                  case ((factorMatrix1BitArrayRowIndex0: Int, (rowSummationHashTableRangeStart: Int, rowSummationHashTableRangeEnd: Int)),
                  partRowwiseArray: Array[Array[Int]]) =>
                    for (rowIndex0 <- 0 until targetFactorMatrixRowLength) {
                      val unfoldedTensorRowPiece = partRowwiseArray(rowIndex0)

                      /** START of error computation */
                      assert(unfoldedTensorRowPiece == null || unfoldedTensorRowPiece.forall(x => x < matrix2RowLength)) // check the range of elements in unfoldedTensorRowPiece
                      val targetFactorBitRow = targetBitRowFactorMatrix(rowIndex0)

                      for (tryVal <- bitRowMasks.indices) {
                        val tryTargetFactorBitRow = setBits(targetFactorBitRow.clone().asInstanceOf[JBitSet], rank, colsStartIndex0, colsEndIndex0, tryVal, bitRowMasks)._1
                        val factorMatrix1BitRow = bitRowFactorMatrix1(factorMatrix1BitArrayRowIndex0)
                        tryTargetFactorBitRow.and(factorMatrix1BitRow)
                        val rowSummationsArray: Array[Array[Array[Int]]] = summationMapForRowwiseMatrix2((rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd))
                        val constructedTensorRowPiece = constructTensorRow(tryTargetFactorBitRow, rowSummationsArray, rank, rangeSplitSizes, maxRankSplitSize)

                        // compute the difference between the constructed tensor row and the corresponding row piece of unfolded input tensor
                        val distance = computeTensorRowDistance(constructedTensorRowPiece, unfoldedTensorRowPiece)
                        errorVectors(tryVal)(rowIndex0) += distance
                      }
                      /** END of error computation */
                    }
                }
                iterCount += 1
            }
            if (iterCount == 0) {
              throw new Exception(s"ERROR: partitionIndex=$partitionIndex is empty. Try again with a smaller value for the '--num-unfolded-tensor-partitions' parameter.")
            }
            assert(iterCount == 1, s"partitionIndex: $partitionIndex, iterCount: $iterCount") // assert an assumption that only one (logical) partition is sent to each task.

            partitionErrorVectors(errorVectors, numUnfoldedTensorPartitions).toIterator
          }
        }
      /** END of RDD.mapPartitions **/

      updateFactorMatrixColumns(partitionedErrorVectorRDD, newColumnsVector, targetBitRowFactorMatrix, rank,
        colsStartIndex0, colsEndIndex0, errorDeltaVector, tablesOfNumZeros, probForZeroForTieBreaking, maxZeroPercentage)
      val _sumErrorDelta = errorDeltaVector.sum

      // update target factor matrix with the new column vector.
      val targetBitRowFactorMatrixString: Option[Array[String]] =
        if (DEBUG) {
          Some(targetBitRowFactorMatrix.map { bitRow => bitSetToBinaryString(bitRow, rank) })
        } else {
          None
        }

      val _numBitUpdates = updateTargetBitRowFactorMatrix(targetBitRowFactorMatrix, rank, newColumnsVector, colsStartIndex0, colsEndIndex0, bitRowMasks)

      if (DEBUG) {
        targetBitRowFactorMatrixString.get.zip(targetBitRowFactorMatrix) foreach {
          case (beforeStr, after) => DBTFDriver.log(s"$beforeStr -> ${bitSetToBinaryString(after, rank)}")
        }
      }

      sumErrorDelta += _sumErrorDelta
      numBitUpdates += _numBitUpdates
    }
    /** END of column-wise update loop */

    summationMapRDD.unpersist(blocking = false)

    val targetFactorMatrix: Array[(Int, Int)] = MatrixOperation.bitRowToSparseMatrix(targetBitRowFactorMatrix, rank)
    DBTFDriver.log(s"\tNumber of nonzeros: ${targetFactorMatrix.length} (${targetFactorMatrix.length / (targetFactorMatrixDim._1 * targetFactorMatrixDim._2).toDouble})")
    (targetFactorMatrix, numBitUpdates, sumErrorDelta)
  }

  /**
    * Build an RDD of summation map that contains summation map(s) for rowwise matrix necessary for each partition.
    *
    * @param partUnfoldedTensorMapBasedRDD RDD of partitioned unfolded tensor
    * @param brAllPairsRowSummations broadcast variable for all pairs of row summations
    * @param matrix2RowLength row length of matrix2
    * @return summation map
    */
  //noinspection ScalaUnusedSymbol
  def buildSummationMapForRowwiseMatrix(partUnfoldedTensorMapBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Map[Long, Array[Int]])])]],
                                        partUnfoldedTensorArrayBasedRDD: Option[RDD[(Int, Array[((Int, (Int, Int)), Array[Array[Int]])])]],
                                        brAllPairsRowSummations: Broadcast[Array[Array[Array[Int]]]], matrix2RowLength: Int): RDD[(Int, mutable.HashMap[(Int, Int), Array[Array[Array[Int]]]])] = {
    // row summation ranges for each partition id
    val partRowSummationRangeRDD: RDD[(Int, Array[(Int, Int)])] =
      if (partUnfoldedTensorMapBasedRDD.isDefined) {
        partUnfoldedTensorMapBasedRDD.get mapValues {
          case (groupedPartRowwiseMap: Array[((Int, (Int, Int)), Map[Long, Array[Int]])]) =>
            (groupedPartRowwiseMap map {
              case ((__factorMatrix1BitArrayRowIndex0__, (rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd)), __partRowwiseMap__) =>
                (rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd)
            }).distinct
        }
      } else {
        partUnfoldedTensorArrayBasedRDD.get mapValues {
          case (groupedPartRowwiseArray: Array[((Int, (Int, Int)), Array[Array[Int]])]) =>
            (groupedPartRowwiseArray map {
              case ((__factorMatrix1BitArrayRowIndex0__, (rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd)), __partRowwiseArray__) =>
                (rowSummationHashTableRangeStart, rowSummationHashTableRangeEnd)
            }).distinct
        }
      }

    val summationMap: RDD[(Int, mutable.HashMap[(Int, Int), Array[Array[Array[Int]]]])] = partRowSummationRangeRDD mapValues {
      partRowSummationRanges: Array[(Int, Int)] =>
        assert(partRowSummationRanges.length <= 3, s"partRowSummationRanges.length: ${partRowSummationRanges.length}" + s"partRowSummationRanges: ${partRowSummationRanges.mkString(", ")}")
        val allPairsRowSummations = brAllPairsRowSummations.value
        val summationMapForRowwiseMatrix = mutable.HashMap.empty[(Int, Int), Array[Array[Array[Int]]]]

        partRowSummationRanges foreach {
          rangeTuple =>
            val (rangeStart, rangeEnd) = rangeTuple
            if (rangeStart == 0 && rangeEnd == matrix2RowLength - 1) { // full range. no need to slice.
              summationMapForRowwiseMatrix.put(rangeTuple, allPairsRowSummations)
            } else {
              assert(!summationMapForRowwiseMatrix.contains(rangeTuple))
              summationMapForRowwiseMatrix.put(rangeTuple, allPairsRowSummations map {
                allPairsRowSummation => sliceRowwiseMatrix(allPairsRowSummation, rangeStart, rangeEnd)
              })
            }
        }

        summationMapForRowwiseMatrix
    }

    summationMap
  }

  /**
    * Vertically slice the given rowwise sparse matrix according to the given column range
    *
    * @param rowwiseMatrix rowwise sparse matrix
    * @param fromColumnIndex start column index (inclusive)
    * @param toColumnIndex end column index (inclusive)
    * @return sliced rowwise sparse matrix
    */
  def sliceRowwiseMatrix(rowwiseMatrix: Array[Array[Int]], fromColumnIndex: Int, toColumnIndex: Int): Array[Array[Int]] = {
    rowwiseMatrix.map(_.filter(e => e >= fromColumnIndex && e <= toColumnIndex))
  }

  /**
    * Partition the given error vectors of the same length into the following number of ranges
    * 1. error vector length >= number of partitions: as many as the number of partitions
    * 2. error vector length < number of partitions: as many as the error vector length (i.e., each range is to be of length 1)
    *
    * @param errorVectors array of error vector. error vector at index i contains errors for the bit sequence i.
    * @param numUnfoldedTensorPartitions  number of partitions of unfolded tensors
    * @return partitioned error vectors, i.e., array of (part range, part of error vectors)
    */
  def partitionErrorVectors(errorVectors: Array[Array[Int]], numUnfoldedTensorPartitions: Int): Array[((Int, Int), Array[Array[Int]])] = {
    assert(errorVectors.map(_.length).distinct.length == 1) // length of all error vectors should be identical.
    assert(numUnfoldedTensorPartitions > 0)

    val errorVecLength = errorVectors(0).length
    val numPartitions = math.min(errorVecLength, numUnfoldedTensorPartitions)
    val partitionRangeLength = Math.ceil(errorVecLength.toDouble / numPartitions).toInt
    assert(partitionRangeLength > 0)

    var rangeStart = 0
    val rangeEndMax = errorVecLength - 1
    val partitionedErrorVectors = new Array[((Int, Int), Array[Array[Int]])](numPartitions)

    for (i <- 0 until numPartitions) {
      val rangeEnd = Math.min(rangeStart + partitionRangeLength - 1, rangeEndMax)
      val subErrorVectors = new Array[Array[Int]](errorVectors.length)
      for (k <- errorVectors.indices) {
        subErrorVectors(k) = errorVectors(k).slice(from = rangeStart, until = rangeEnd + 1)
      }

      partitionedErrorVectors(i) = ((rangeStart, rangeEnd), subErrorVectors)
      rangeStart = rangeEnd + 1
    }

    partitionedErrorVectors
  }

  /**
    * Split the given bit row into smaller bit rows according to the given split sizes.
    *
    * @param bitRow bit row
    * @param rangeSplitSizes array of rank split sizes
    * @param maxSplitSize maximum size of a split
    * @return array of split bit rows
    */
  def splitBitRowByRange(bitRow: JBitSet, rangeSplitSizes: Array[Int], maxSplitSize: Int): Array[JBitSet] = {
    assert(rangeSplitSizes.sum >= bitRow.length(), s"${rangeSplitSizes.mkString(", ")} < ${bitRow.length()}")
    assert(rangeSplitSizes.forall(_ <= maxSplitSize), s"rangeSplitSizes: ${rangeSplitSizes.mkString(", ")}, maxSplitSize: $maxSplitSize")

    val splitBitRows = new Array[JBitSet](rangeSplitSizes.length)
    var toIndex = rangeSplitSizes.sum
    for (i <- rangeSplitSizes.indices) {
      val rangeSplitSize = rangeSplitSizes(i)
      val fromIndex = toIndex - rangeSplitSize
      splitBitRows(i) = bitRow.get(fromIndex, toIndex)
      toIndex = fromIndex
    }
    assert(toIndex == 0, toIndex)

    splitBitRows
  }

  /**
    * Transform a bit set into a corresponding binary string.
    *
    * @param bitSet bit set
    * @param rank rank
    * @return binary string corresponding to the given bit set
    */
  def bitSetToBinaryString(bitSet: JBitSet, rank: Int): String = {
    val sb = new StringBuilder("0" * rank)
    var i = bitSet.nextSetBit(0)
    while (i >= 0) {
      val colIndex0 = (rank - 1) - i
      sb.setCharAt(colIndex0, '1')
      i = bitSet.nextSetBit(i + 1)
    }

    sb.toString
  }

  /**
    * Transform a bit set into a corresponding integer value.
    *
    * @param bitSet bit set
    * @return an integer value corresponding to the given bit set
    */
  def bitSetToInt(bitSet: JBitSet): Int = {
    assert(bitSet.length() <= 32, bitSet.length())
    if (bitSet.length == 0) {
      0
    } else {
      val l = bitSet.toLongArray
      assert(l.length == 1, l.length)
      l(0).toInt
    }
  }

  /**
    * Construct tensor bit row using the cache containing pre-computed row summations.
    * If rank is greater than the maxRankSplitSize, multiple rows need to be merged to obtain the tensor row.
    *
    * @param cacheKeyBitRow bit row used as a cache key
    * @param rowSummationsArray array of row summations
    * @param rank rank
    * @param rangeSplitSizes array of range split sizes
    * @param maxRankSplitSize maximum number of rows out of which to build a single cache table
    * @return sparse tensor row
    */
  def constructTensorRow(cacheKeyBitRow: JBitSet, rowSummationsArray: Array[Array[Array[Int]]],
                         rank: Int, rangeSplitSizes: Array[Int], maxRankSplitSize: Int): Array[Int] = {
    assert(rangeSplitSizes.sum == rank)

    if (rowSummationsArray.length == 1) {
      assert(cacheKeyBitRow.length() <= maxRankSplitSize, s"targetFactorBitRow: $cacheKeyBitRow, maxRankSplitSize: $maxRankSplitSize")
      assert(Array(0, 1).contains(cacheKeyBitRow.toLongArray.length), "targetFactorBitRow: [" + cacheKeyBitRow.toLongArray.mkString(", ") + "]")
      try {
        rowSummationsArray(0)(bitSetToInt(cacheKeyBitRow))
      } catch {
        case e: Throwable =>
          println(rowSummationsArray(0).length)
          println(bitSetToInt(cacheKeyBitRow))
          throw e
      }

    } else {
      val splitBitRows = splitBitRowByRange(cacheKeyBitRow, rangeSplitSizes, maxSplitSize = maxRankSplitSize)
      assert(splitBitRows.forall(_.length <= maxRankSplitSize), splitBitRows.map(_.length).mkString(", "))
      var tensorRow = rowSummationsArray(0)(bitSetToInt(splitBitRows(0)))
      for (i <- 1 until rowSummationsArray.length) {
        tensorRow = mergeRows(tensorRow, rowSummationsArray(i)(bitSetToInt(splitBitRows(i))))
      }
      tensorRow
    }
  }

  /**
    * Merge two sparse rows into a single sparse row.
    *
    * @param row1 sparse row
    * @param row2 sparse row
    * @return single sparse row that merges the two sparse rows
    */
  def mergeRows(row1: Array[Int], row2: Array[Int]): Array[Int] = {
    assert(row1.sorted sameElements row1, s"row1: ${row1.mkString(", ")}")
    assert(row2.sorted sameElements row2, s"row2: ${row2.mkString(", ")}")

    var i1, i2 = 0
    val _mergedRow = new Array[Int](row1.length + row2.length)
    var idx = 0

    while (i1 < row1.length && i2 < row2.length) {
      val v1 = row1(i1)
      val v2 = row2(i2)
      if (v1 > v2) {
        _mergedRow(idx) = v2
        idx += 1
        i2 += 1
      } else if (v1 < v2) {
        _mergedRow(idx) = v1
        idx += 1
        i1 += 1
      } else {
        _mergedRow(idx) = v1
        idx += 1
        i1 += 1
        i2 += 1
      }
    }

    for (i <- i1 until row1.length) {
      _mergedRow(idx) = row1(i)
      idx += 1
    }

    for (i <- i2 until row2.length) {
      _mergedRow(idx) = row2(i)
      idx += 1
    }

    val mergedRow = new Array[Int](idx)
    Array.copy(_mergedRow, 0, mergedRow, 0, idx)
    assert(mergedRow.sorted sameElements mergedRow, s"mergedRow: ${mergedRow.mkString(", ")}")
    mergedRow
  }

  /**
    * Split the given range into an array of smaller ranges of approximately equal size
    * such that the length of each subrange does not exceed the given maxSplitSize.
    *
    * @param rangeStartIndex start index (inclusive) of a range
    * @param rangeEndIndex   end index (inclusive) of a range
    * @param maxSplitSize    maximum size of a split
    * @return an array of non-overlapping subranges
    */
  def splitRangeWithMaxSplitSize(rangeStartIndex: Int, rangeEndIndex: Int, maxSplitSize: Int): Array[(Int, Int)] = {
    assert(rangeStartIndex <= rangeEndIndex, (rangeStartIndex, rangeEndIndex))
    assert(maxSplitSize > 0)

    val rangeLength = (rangeEndIndex - rangeStartIndex + 1).toDouble
    val (numSplits, splitLength) = if (rangeLength <= maxSplitSize) {
      (1, rangeLength)
    } else {
      val numSplits = math.ceil(rangeLength / maxSplitSize).toInt
      (numSplits, rangeLength / numSplits)
    }
    assert(numSplits >= 1)
    assert(splitLength >= 1 && splitLength <= maxSplitSize)

    val rangeSplits = new Array[(Int, Int)](numSplits)
    var splitLengthSum = rangeStartIndex.toDouble
    for (i <- 0 until numSplits) {
      if (i == numSplits - 1) {
        rangeSplits(i) = (splitLengthSum.toInt, rangeEndIndex)
      } else {
        rangeSplits(i) = (splitLengthSum.toInt, (splitLengthSum + splitLength - 1).toInt)
        splitLengthSum = splitLengthSum + splitLength
      }
    }
    assert(rangeSplits.last._2 == rangeEndIndex, rangeSplits.last)

    rangeSplits
  }

  /**
    * Split the given range into an array of smaller ranges of approximately equal size
    * such that the number of smaller ranges is equal to the given numSplits.
    *
    * @param rangeStartIndex start index (inclusive) of a range
    * @param rangeEndIndex   end index (inclusive) of a range
    * @param numSplits       number of splits
    * @return an array of non-overlapping subranges
    */
  def splitRangeWithNumSplits(rangeStartIndex: Int, rangeEndIndex: Int, numSplits: Int): Array[(Int, Int)] = {
    assert(rangeStartIndex <= rangeEndIndex, (rangeStartIndex, rangeEndIndex))
    assert(numSplits > 0, numSplits)
    assert(rangeEndIndex - rangeStartIndex + 1 >= numSplits)

    val rangeLength: Double = (rangeEndIndex - rangeStartIndex + 1).toDouble
    val splitLength: Double = rangeLength / numSplits

    val rangeSplits = new Array[(Int, Int)](numSplits)
    var splitLengthSum = rangeStartIndex.toDouble
    for (i <- 0 until numSplits) {
      if (i == numSplits - 1) {
        rangeSplits(i) = (splitLengthSum.toInt, rangeEndIndex)
      } else {
        rangeSplits(i) = (splitLengthSum.toInt, (splitLengthSum + splitLength - 1).toInt)
        splitLengthSum = splitLengthSum + splitLength
      }
    }
    assert(rangeSplits.last._2 == rangeEndIndex, rangeSplits.last)

    rangeSplits
  }

  /**
    * Given a rowwise sparse matrix, return an array containing all possible row summations.
    *
    * @param rowwiseMatrix rowwise sparse matrix
    * @param maxRankSplitSize maximum size to split the rank
    * @return array containing all possible row summations
    */
  def computeRowSummationForAllPairs(rowwiseMatrix: Array[Array[Int]], maxRankSplitSize: Int): Array[Array[Array[Int]]] = {
    val rank = rowwiseMatrix.length
    val rangeSplits = splitRangeWithMaxSplitSize(0, rank - 1, maxRankSplitSize)

    val rowSummationsArray = new Array[Array[Array[Int]]](rangeSplits.length)
    for ((rangeSplit, idx) <- rangeSplits.zipWithIndex) {
      val (rankSplitStartIndex, rankSplitEndIndex) = rangeSplit
      val rankSplitSize = rankSplitEndIndex - rankSplitStartIndex + 1

      val numAllPairs = 1 << rankSplitSize // 2 ^ rankSplitSize
      val rowSummations = new Array[Array[Int]](numAllPairs)
      rowSummationsArray(idx) = rowSummations

      var i = rankSplitSize - 1
      var iVal = 1
      rowSummations(0) = Array[Int]()
      while (iVal < numAllPairs) {
        rowSummations(iVal) = rowwiseMatrix(rankSplitStartIndex + i)
        i -= 1
        iVal = iVal << 1
      }
      assert(i == -1, s"i: $i")

      for (i <- 0 until numAllPairs) {
        if (rowSummations(i) == null) {
          val (rowSumPart1, rowSumPart2) = {
            if (i % 2 == 1) {
              (rowSummations(i - 1), rowSummations(1))
            } else {
              var n, next = 1
              while (next <= i) {
                // find the number composed only of the most significant bit
                n = next
                next = next << 1
              }
              (rowSummations(n), rowSummations(i - n))
            }
          }
          rowSummations(i) = mergeRows(rowSumPart1, rowSumPart2)
        }
      }
    }

    rowSummationsArray
  }

  /**
    * Compute the bitwise distance between two tensor rows.
    *
    * @param tensorRow1 sparse tensor row
    * @param tensorRow2 sparse tensor row
    * @return bitwise distance between two tensor rows
    */
  def computeTensorRowDistance(tensorRow1: Array[Int], tensorRow2: Array[Int]): Int = {
    assert(tensorRow1 == null || (tensorRow1.sorted sameElements tensorRow1), tensorRow1.mkString(", "))
    assert(tensorRow2 == null || (tensorRow2.sorted sameElements tensorRow2), tensorRow2.mkString(", "))

    if (tensorRow1 == null) {
      if (tensorRow2 == null) {
        return 0
      } else {
        return tensorRow2.length
      }
    }

    if (tensorRow2 == null) {
      return tensorRow1.length
    }

    // both tensorRow1 and tensorRow2 are not null.
    var distance = 0
    var i1, i2 = 0

    while (i1 < tensorRow1.length && i2 < tensorRow2.length) {
      val v1 = tensorRow1(i1)
      val v2 = tensorRow2(i2)
      if (v1 > v2) {
        i2 += 1
        distance += 1
      } else if (v1 < v2) {
        i1 += 1
        distance += 1
      } else {
        i1 += 1
        i2 += 1
      }
    }

    // add the number of remaining elements
    distance + {
      if (i1 < tensorRow1.length && i2 >= tensorRow2.length) {
        tensorRow1.length - i1
      } else if (i2 < tensorRow2.length && i1 >= tensorRow1.length) {
        tensorRow2.length - i2
      } else { // none remaining
        assert(i1 >= tensorRow1.length && i2 >= tensorRow2.length)
        0
      }
    }
  }

  // Tried to reduce the number of comparison at each iteration.
  // However, this change does not seem to bring performance improvement.
  /**
    * Compute the bitwise distance between two tensor rows.
    *
    * @param tensorRow1 sparse tensor row
    * @param tensorRow2 sparse tensor row
    * @return bitwise distance between two tensor rows
    */
  def computeTensorRowDistanceVer2(tensorRow1: Array[Int], tensorRow2: Array[Int]): Int = {
    assert(tensorRow1.sorted sameElements tensorRow1)
    assert(tensorRow2.sorted sameElements tensorRow2)

    if (tensorRow1.length == 0 && tensorRow2.length == 0) {
      return 0
    }

    var distance = 0
    var i1, i2 = 0
    var keepGoing = if (tensorRow1.length != 0 && tensorRow2.length != 0) true else false // false if either of the two rows is empty.

    while (keepGoing) {
      val v1 = tensorRow1(i1)
      val v2 = tensorRow2(i2)
      if (v1 > v2) {
        i2 += 1
        distance += 1
        if (i2 >= tensorRow2.length) keepGoing = false
      } else if (v1 < v2) {
        i1 += 1
        distance += 1
        if (i1 >= tensorRow1.length) keepGoing = false
      } else {
        i1 += 1
        i2 += 1
        if (i1 >= tensorRow1.length || i2 >= tensorRow2.length) keepGoing = false
      }
    }

    // add the number of remaining elements
    distance + {
      if (i1 < tensorRow1.length && i2 >= tensorRow2.length) {
        tensorRow1.length - i1
      } else if (i2 < tensorRow2.length && i1 >= tensorRow1.length) {
        tensorRow2.length - i2
      } else { // none remaining
        assert(i1 >= tensorRow1.length && i2 >= tensorRow2.length)
        0
      }
    }
  }

  /**
    * Set bits in the specified column range of the given bit row factor matrix to the given column values.
    *
    * @param targetBitRowFactorMatrix bit row factor matrix (array of BitSet)
    * @param rank rank
    * @param newColumnsVector new columns vector
    * @param colsStartIndex0 zero-based start index of columns
    * @param colsEndIndex0 zero-based end index of columns
    * @param bitRowMasks array of bit row masks
    * @return number of bit upates
    */
  def updateTargetBitRowFactorMatrix(targetBitRowFactorMatrix: Array[JBitSet], rank: Int, newColumnsVector: Array[Int],
                                     colsStartIndex0: Int, colsEndIndex0: Int, bitRowMasks: Array[JBitSet]): Int = {
    assert(newColumnsVector.forall(e => e >= 0), newColumnsVector.mkString(", "))
    assert(targetBitRowFactorMatrix.length == newColumnsVector.length)
    assert(targetBitRowFactorMatrix.forall(bitRow => bitRow.length <= rank), {targetBitRowFactorMatrix.mkString("\n") + s", rank: $rank"})

    var numUpdates = 0
    for (i <- targetBitRowFactorMatrix.indices) {
      val bitRow = targetBitRowFactorMatrix(i)
      val columnsValue = newColumnsVector(i)
      val (_, numUpdatesByRow) = setBits(bitRow, rank, colsStartIndex0, colsEndIndex0, columnsValue, bitRowMasks, countNumUpdates = true)
      numUpdates += numUpdatesByRow.get
    }

    numUpdates
  }

  /**
    * Transform the given sparse factor matrix into a map.
    *
    * whose key is the row index, and whose value is the entries belonging to the corresponding row
    * @param targetFactorMatrix target factor matrix in a sparse format
    * @return map whose key is the row index, and whose value is the entries belonging to the corresponding row
    */
  def targetFactorMatrixIntoMapByRow(targetFactorMatrix: Array[(Int, Int)]): Map[Int, Array[Int]] = {
    targetFactorMatrix groupBy (_._1) mapValues { elArray => elArray map (_._2) }
  }

  /**
    * Update the specified columns of a factor matrix to the values that give the minimum error.
    *
    * @param partitionedErrorVectorRDD RDD of partitioned error vector
    * @param newColumnVectors new column vectors
    * @param targetBitRowFactorMatrix target bit row factor matrix
    * @param rank rank
    * @param colsStartIndex0 zero-based start index of columns
    * @param colsEndIndex0 zero-based end index of columns
    * @param errorDeltaVector array of error delta
    * @param tablesOfNumZeros table mapping a value to the number of zeros in it
    * @param probForZeroForTieBreaking probability to choose zero when several column values (including zero) have the same error delta
    * @param maxZeroPercentage maximum percentage of zeros in a factor matrix
    */
  def updateFactorMatrixColumns(partitionedErrorVectorRDD: RDD[((Int, Int), Array[Array[Int]])], newColumnVectors: Array[Int],
                                targetBitRowFactorMatrix: Array[JBitSet], rank: Int, colsStartIndex0: Int, colsEndIndex0: Int,
                                errorDeltaVector: Array[Long], tablesOfNumZeros: Array[Array[Int]], probForZeroForTieBreaking: Double = 0.5,
                                maxZeroPercentage: Double = 0.95): Unit = {
    assert(probForZeroForTieBreaking >= 0 && probForZeroForTieBreaking <= 1.0, probForZeroForTieBreaking)
    assert(maxZeroPercentage >= 0.0 && maxZeroPercentage <= 1.0, maxZeroPercentage)

    val partitionedError: Map[(Int, Int), Array[Array[Int]]] = partitionedErrorVectorRDD reduceByKey {
      case (errorVectorsA: Array[Array[Int]], errorVectorsB: Array[Array[Int]]) =>
        assert(errorVectorsA.length == errorVectorsB.length)
        assert((errorVectorsA.length & (errorVectorsA.length - 1)) == 0, s"errorVectorsA.length: ${errorVectorsA.length} is not a power of two.")
        errorVectorsA.zip(errorVectorsB) map {
          case (errorVectorA: Array[Int], errorVectorB: Array[Int]) =>
            assert(errorVectorA.length == errorVectorB.length)
            (errorVectorA, errorVectorB).zipped.map(_ + _)
        }
    } collectAsMap()

    val sortedKeys = partitionedError.keys.toArray.sorted
    assert(sortedKeys(0)._1 == 0, s"error partition range should start from 0, but it starts from ${sortedKeys(0)._1}")

    val numErrorVectors = partitionedError.values.head.length
    val errorVectors = new Array[mutable.ArrayBuffer[Int]](numErrorVectors)
    for (i <- errorVectors.indices) {
      errorVectors(i) = mutable.ArrayBuffer.empty[Int]
    }

    sortedKeys foreach { partRange =>
      val (partRangeStart, partRangeEnd) = partRange
      val partErrorVectors: Array[Array[Int]] = partitionedError(partRange)
      assert(partErrorVectors.map(_.length).distinct.length == 1) // partErrorVectors(i).length should be identical.
      assert(partRangeEnd - partRangeStart + 1 == partErrorVectors(0).length, s"${partRangeEnd - partRangeStart + 1} != ${partErrorVectors(0).length}")

      partErrorVectors.zipWithIndex.foreach { case (partErrorVector, index) => errorVectors(index) ++= partErrorVector }
    }

    val numColsToUpdate = colsEndIndex0 - colsStartIndex0 + 1
    assert(numColsToUpdate >= 1, numColsToUpdate)
    updateNewColumnVectors(errorVectors.map(_.toArray), newColumnVectors, tablesOfNumZeros, numColsToUpdate, probForZeroForTieBreaking, maxZeroPercentage)

    for (i <- newColumnVectors.indices) {
      val targetBitRow = targetBitRowFactorMatrix(i)
      val curColumnsValue = bitSetToInt(getBits(targetBitRow, rank, colsStartIndex0, colsEndIndex0))
      val curColumnsValueError = errorVectors(curColumnsValue)(i)
      val updatedError = errorVectors(newColumnVectors(i))(i)
      errorDeltaVector(i) = updatedError - curColumnsValueError
    }
  }

  def updateNewColumnVectors(errorVectors: Array[Array[Int]], newColumnVectors: Array[Int], tablesOfNumZeros: Array[Array[Int]],
                             numColsToUpdate: Int, probForZeroForTieBreaking: Double = 0.5, maxZeroPercentage: Double = 0.95): Unit = {
    assert(errorVectors.map(_.length).distinct.length == 1) // errorVectors(i).length should be identical for all i.
    assert(errorVectors.map(_.length).distinct(0) == newColumnVectors.length)
    assert(maxZeroPercentage >= 0.0 && maxZeroPercentage <= 1.0, maxZeroPercentage)

    val priorityQueue = mutable.PriorityQueue.empty[(Long, Int, Int)](Ordering.by((_: (Long, Int, Int))._1).reverse)
    for (i <- errorVectors(0).indices) { // go over the rows of a factor matrix
      var minError = Long.MaxValue
      var minColumnsValue = -1

      for (columnsValue <- errorVectors.indices) { // find the columnsValue that gave the minimum error
        if (errorVectors(columnsValue)(i) < minError) {
          minError = errorVectors(columnsValue)(i)
          minColumnsValue = columnsValue
        }
      }

      val minColumnsValues: Array[Int] = errorVectors.zipWithIndex.map {
        case (errorVector, columnsValue) => (columnsValue, errorVector(i))
      } filter { case (columnsValue, error) => error == minError } map(_._1)
      assert(minColumnsValues.length >= 1, s"${minColumnsValues.mkString(", ")}")

      if (minColumnsValues.contains(0) && minColumnsValues.length > 1) {
        assert(minColumnsValues(0) == 0)
        if (random.nextFloat() <= probForZeroForTieBreaking) {
          minColumnsValue = 0
        } else {
          val minColumnsValuesWithoutZero = minColumnsValues.slice(1, minColumnsValues.length)
          minColumnsValue = minColumnsValuesWithoutZero(random.nextInt(minColumnsValuesWithoutZero.length))
        }
      } else {
        minColumnsValue = minColumnsValues(random.nextInt(minColumnsValues.length))
      }

      newColumnVectors(i) = minColumnsValue

      val otherColumnsValues = errorVectors.indices.toArray.filter(_ != minColumnsValue)
      val errorDiffsOfOtherColumnsValues = errorVectors.zipWithIndex.map {
        case (errorVector, columnsValue) => (columnsValue, errorVector(i) - minError)
      } filter(_._1 != minColumnsValue)

      otherColumnsValues.zip(errorDiffsOfOtherColumnsValues) foreach {
        case (otherColumnsValue, (columnsValue, errorDiff)) =>
          assert(otherColumnsValue == columnsValue)
          priorityQueue += ((errorDiff, otherColumnsValue, i))
      }
    }
    assert(priorityQueue.size == newColumnVectors.length * ((1 << numColsToUpdate) - 1),
      s"${priorityQueue.size} != ${(newColumnVectors.length - 1) * (1 << numColsToUpdate)}")

    val tableOfNumZeros = tablesOfNumZeros(numColsToUpdate - 1)
    val numMaxZeros = (newColumnVectors.length * numColsToUpdate * maxZeroPercentage).toInt
    if (numMaxZeros == 0) DBTFDriver.log(s"Warn: numMaxZeros is set to 0 with maxZeroPercentage=$maxZeroPercentage for numColsToUpdate=$numColsToUpdate, and newColumnVectors of length ${newColumnVectors.length}.")
    val curNumZeros = newColumnVectors.map(v => tableOfNumZeros(v))
    val numZeros = curNumZeros.sum

    if (numZeros > numMaxZeros) {
      var numExcessiveZeros = numZeros - numMaxZeros

      while (numExcessiveZeros > 0) {
        val (_, columnsValue, index) = priorityQueue.dequeue()

        if (tableOfNumZeros(columnsValue) < curNumZeros(index)) {
          newColumnVectors(index) = columnsValue

          val delta = curNumZeros(index) - tableOfNumZeros(columnsValue)
          curNumZeros(index) = tableOfNumZeros(columnsValue)
          numExcessiveZeros -= delta
        }
      }
    }
  }

  /**
    * Generate a randomly initialized factor matrix.
    *
    * @param factorMatrixDimension row and column dimension of a factor matrix
    * @param density matrix density
    * @param baseIndex base index
    * @param random random number generator
    * @return randomly initialized factor matrix
    */
  def initializeFactorMatrixRandomly(factorMatrixDimension: (Int, Int), density: Double, baseIndex: Int = 0,
                                     random: scala.util.Random = scala.util.Random): Array[(Int, Int)] = {
    val randomSparseTensor = TensorOperation.generateRandomTensor(Array[Int](factorMatrixDimension._1, factorMatrixDimension._2), density, baseIndex, random)
    randomSparseTensor.map { indices => (indices(0), indices(1)) }
  }

  /**
    * Create an array of all possible BitSets of the given length where only a single bit is turned on.
    *
    * @param bitLength bit length
    * @return single bit array
    */
  def prepareSingleBitArray(bitLength: Int): Array[JBitSet] = {
    val singleBitArray = new Array[JBitSet](bitLength)
//    var bit: Long = 1L
    for (i <- 0 until bitLength) {
      val bitSet = new JBitSet()
      bitSet.set(i)

      singleBitArray(i) = bitSet
    }
    singleBitArray
  }

  /**
    * Generate bit row masks.
    *
    * @param colsStartIndex0 zero-based start index of columns
    * @param colsEndIndex0 zero-based end index of columns
    * @param rank rank
    * @return array of bit row masks
    */
  def generateBitRowMasks(colsStartIndex0: Int, colsEndIndex0: Int, rank: Int): Array[JBitSet] = {
    assert(colsEndIndex0 >= colsStartIndex0)
    assert(colsEndIndex0 - colsStartIndex0 + 1 <= rank)

    Range(0, 1 << (colsEndIndex0 - colsStartIndex0 + 1)).map { x =>
      val bitSet = new JBitSet(rank)
      var bitmask = 1
      for (i <- 0 to (colsEndIndex0 - colsStartIndex0)) {
        if ((bitmask & x) > 0) {
          bitSet.set(rank - 1 - colsEndIndex0 + i)
        }
        bitmask = bitmask << 1
      }
      bitSet
    }.toArray
  }

  /**
    * Set bits at the specified indices of the given bit row to the given bit value, and
    * return the updated bit row along with the number of bit updates if countNumUpdates is set to true.
    *
    * @param bitRow bit row
    * @param bitRowLength length of bit row
    * @param colsStartIndex0 zero-based start index of columns
    * @param colsEndIndex0 zero-based end index of columns
    * @param bitValue bit value to set
    * @param bitRowMasks array of bit row masks
    * @param countNumUpdates whether to count number of updates
    * @return (updated bit row, number of bit updates (optional))
    */
  def setBits(bitRow: JBitSet, bitRowLength: Int, colsStartIndex0: Int, colsEndIndex0: Int, bitValue: Int, bitRowMasks: Array[JBitSet],
              countNumUpdates: Boolean = false): (JBitSet, Option[Int]) = {
    assert(colsStartIndex0 >= 0 && colsEndIndex0 >= 0 && colsStartIndex0 <= colsEndIndex0)
    assert((colsEndIndex0 - colsStartIndex0 + 1) <= bitRowLength)
    assert(bitValue >= 0 && bitValue < math.pow(2, colsEndIndex0 - colsStartIndex0 + 1).toInt,
      s"bitValue: $bitValue, colsStartIndex0: $colsStartIndex0, colsEndIndex0: $colsEndIndex0")

    val colsLength = colsEndIndex0 - colsStartIndex0 + 1

    if (colsLength == 1) {
      assert(bitValue == 0 || bitValue == 1)
      val booleanBitValue = bitValue == 1
      val bitIndexToUpdate = bitRowLength - 1 - colsStartIndex0

      val numUpdates = if (countNumUpdates) {
        val oldValue = bitRow.get(bitIndexToUpdate)
        bitRow.set(bitIndexToUpdate, booleanBitValue)
        Some(if (oldValue ^ booleanBitValue) 1 else 0)
      } else {
        bitRow.set(bitIndexToUpdate, booleanBitValue)
        None
      }

      (bitRow, numUpdates)
    } else {
      val bitRowClone = if (countNumUpdates) Some(bitRow.clone().asInstanceOf[JBitSet]) else None
      val fromIndex = bitRowLength - 1 - colsEndIndex0
      val toIndex = bitRowLength - colsStartIndex0

      bitRow.clear(fromIndex, toIndex)
      bitRow.or(bitRowMasks(bitValue))

      val numUpdates = bitRowClone.map { r =>
        r.xor(bitRow)
        r.cardinality
      }

      (bitRow, numUpdates)
    }
  }

  /**
    * Get bits from the given bit row corresponding to the specified indices.
    *
    * @param bitRow bit row
    * @param bitRowLength length of bit row
    * @param colsStartIndex0 zero-based start index of columns
    * @param colsEndIndex0 zero-based end index of columns
    * @return bits from the given bit row corresponding to the specified indices
    */
  def getBits(bitRow: JBitSet, bitRowLength: Int, colsStartIndex0: Int, colsEndIndex0: Int): JBitSet = {
    assert(colsStartIndex0 >= 0 && colsEndIndex0 >= 0 && colsStartIndex0 <= colsEndIndex0)
    assert(colsEndIndex0 - colsStartIndex0 + 1 <= bitRowLength, s"colsLength: ${colsEndIndex0 - colsStartIndex0 + 1}")

    val fromIndex = bitRowLength - 1 - colsEndIndex0
    val toIndex = bitRowLength - colsStartIndex0

    bitRow.get(fromIndex, toIndex)
  }

  def checkIntegerOverflowFromProduct(x: Int, y: Int): Boolean = {
    BigInt(x) * BigInt(y) != x * y
  }

  def buildTableOfNumZerosInBinaryForm(numColsToUpdate: Int): Array[Int] = {
    val bitRowMasks: Array[JBitSet] = generateBitRowMasks(0, numColsToUpdate - 1, numColsToUpdate)
    val table = new Array[Int](bitRowMasks.length)
    bitRowMasks.map(x => bitSetToInt(x)).foreach {
      n => table(n) = getNumZerosInBinaryForm(n, numColsToUpdate)
    }
    table
  }

  def getNumZerosInBinaryForm(x: Int, binaryStringMinLen: Int): Int = {
    var binaryString = x.toBinaryString
    if (binaryString.length < binaryStringMinLen) {
      binaryString = "0" * (binaryStringMinLen - binaryString.length) + binaryString
    }

    binaryString.foldLeft(0) {
      case (z, c) =>
        if (c == '0') {
          z + 1
        } else {
          z
        }
    }
  }
}
