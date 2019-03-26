/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package dbtf

import java.io.{File, FileWriter, PrintWriter}
import java.util.Calendar

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.{OptionParser, RenderingMode}
import util.{MatrixOperation, TensorOperation}

import scala.collection.mutable

/**
  * Driver class for DBTF
  */
object DBTFDriver {
  var logWriter: Option[java.io.PrintWriter] = None
  var resultSummaryWriter: Option[java.io.PrintWriter] = None

  val FACTORIZATION_METHOD_CP = "cp"
  val FACTORIZATION_METHOD_TK = "tk"
  val COMPUTE_ERROR_FOR_EVERY_ITER = "compute_error_for_every_iter"
  val COMPUTE_ERROR_FOR_END_ITER = "compute_error_for_end_iter"
  val UNFOLDED_TENSOR_PARTITIONER_MAP_BASED = "map_based"
  val UNFOLDED_TENSOR_PARTITIONER_ARRAY_BASED = "array_based"

  val defaultTensorFileSeparatorType = "csv"
  val defaultNumInitialCandidateSets = 1
  val defaultConvergeByNumIters = 10
  val defaultConvergeByAbsErrorDeltaValue = 10.0
  val defaultNumConsecutiveItersForConvergeByAbsErrorDelta = 3
  val defaultProbForZeroForTieBreaking = 0.9
  val defaultMaxZeroPercentage = 0.95
  val defaultMaxRankSplitSize = 15

  case class Config (factorizationMethod: String = "", tensorFilePath: String = "", tensorFileSeparatorType: String = defaultTensorFileSeparatorType,
                     baseIndex: Int = -1, rank: Int = -1, rank1: Int = -1, rank2: Int = -1, rank3: Int = -1, randomSeed: Int = -1,
                     mode1Length: Int = -1, mode2Length: Int = -1, mode3Length: Int = -1,
                     numInitialCandidateSets: Int = defaultNumInitialCandidateSets,
                     initialFactorMatrixDensity: Double = -1, initialCoreTensorDensity: Double = -1,
                     convergeByNumIters: Boolean = false, convergeByNumItersValue: Int = defaultConvergeByNumIters,
                     convergeByAbsErrorDelta: Boolean = false, convergeByAbsErrorDeltaValue: Double = -1,
                     numConsecutiveItersForConvergeByAbsErrorDelta: Int = defaultNumConsecutiveItersForConvergeByAbsErrorDelta,
                     probForZeroForTieBreaking: Double = defaultProbForZeroForTieBreaking, maxZeroPercentage: Double = defaultMaxZeroPercentage,
                     maxRankSplitSize: Int = defaultMaxRankSplitSize, unfoldedTensorPartitioner: String = UNFOLDED_TENSOR_PARTITIONER_MAP_BASED,
                     numUnfoldedTensorPartitions: Int = -1, numInputTensorPartitions: Int = -1, computeError: String = "",
                     outputDirPath: String = "", outputFilePrefix: String = "", outputResultSummary: Boolean = false)

  def main(args: Array[String]): Unit = {
    /** Parse arguments */
    val parser: OptionParser[Config] = new scopt.OptionParser[Config]("DBTFDriver") {
      head("DBTF", "version 2.0", " - performs Boolean CP or Tucker factorization, and outputs binary factor matrices and a core tensor (for Tucker factorization)")
      opt[String]("factorization-method").abbr("m").required().action((x, c) => c.copy(factorizationMethod = x))
        .validate(x => if (Array(FACTORIZATION_METHOD_CP, FACTORIZATION_METHOD_TK) contains x) success else failure(s"invalid factorization method: $x"))
        .text("factorization method (cp or tk)")
      opt[Int]('b', "base-index").required().action((x, c) => c.copy(baseIndex = x)).text("base (start) index of a tensor (should be either 0 or 1)").
        validate(x => if (Array(0, 1).contains(x)) success else failure("base-index should be either 0 or 1."))
      opt[Int]("rank").abbr("r").action((x, c) => c.copy(rank = x)).text("rank (required for CP)").
        validate(x => if (x <= 0) failure("rank should be positive.") else success)
      opt[Int]("rank1").abbr("r1").action((x, c) => c.copy(rank1 = x)).text("rank for the first factor (required for Tucker)").
        validate(x => if (x <= 0) failure("rank1 should be positive.") else success)
      opt[Int]("rank2").abbr("r2").action((x, c) => c.copy(rank2 = x)).text("rank for the second factor (required for Tucker)").
        validate(x => if (x <= 0) failure("rank2 should be positive.") else success)
      opt[Int]("rank3").abbr("r3").action((x, c) => c.copy(rank3 = x)).text("rank for the third factor (required for Tucker)").
        validate(x => if (x <= 0) failure("rank3 should be positive.") else success)
      opt[Int]("random-seed").abbr("rs").action((x, c) => c.copy(randomSeed = x)).text("random seed").
        validate(x => if (x < 0) failure("use non-negative random seed.") else success)
      opt[Int]("mode1-length").abbr("m1").required().action((x, c) => c.copy(mode1Length = x)).text("dimensionality of the 1st mode")
      opt[Int]("mode2-length").abbr("m2").required().action((x, c) => c.copy(mode2Length = x)).text("dimensionality of the 2nd mode")
      opt[Int]("mode3-length").abbr("m3").required().action((x, c) => c.copy(mode3Length = x)).text("dimensionality of the 3rd mode")
      opt[Int]("num-initial-candidate-sets").abbr("nic").action((x, c) => c.copy(numInitialCandidateSets = x)).text(s"number of initial sets of factor matrices (and a core tensor) (default=$defaultNumInitialCandidateSets)")
      opt[Double]("initial-factor-matrix-density").abbr("ifd").required().action((x, c) => c.copy(initialFactorMatrixDensity = x)).text("density of an initial factor matrix").
        validate(x => if (x >= 0 && x <= 1) success else failure("initial factor matrix density should be between 0 and 1."))
      opt[Double]("initial-core-tensor-density").abbr("icd").action((x, c) => c.copy(initialCoreTensorDensity = x)).text("density of an initial core tensor (required for Tucker)").
        validate(x => if (x >= 0 && x <= 1) success else failure("initial core tensor density should be between 0 and 1."))
      opt[Int]("converge-by-num-iters").abbr("cni").action((x, c) => c.copy(convergeByNumIters = true, convergeByNumItersValue = x)).
        text(s"marks the execution as converged when the number of iterations reaches the given value (default=$defaultConvergeByNumIters). this is the default method used for checking convergence.").
        validate(x => if (x >= 1) success else failure("The value should be positive."))
      opt[Double]("converge-by-abs-error-delta").abbr("ced").action((x, c) => c.copy(convergeByAbsErrorDelta = true, convergeByAbsErrorDeltaValue = x)).
        text(s"marks the execution as converged when the absolute error delta between two consecutive iterations becomes less than the given value.").
        validate(x => if (x >= 0) success else failure("The value should be non-negative."))
      opt[Int]("num-consecutive-iters-for-converge-by-abs-error-delta").abbr("nci").action((x, c) => c.copy(numConsecutiveItersForConvergeByAbsErrorDelta = x)).
        text(s"when --converge-by-abs-error-delta is set, marks the execution as converged when the absolute error delta was smaller than the specified threshold value for the given number of consecutive iterations (default=$defaultNumConsecutiveItersForConvergeByAbsErrorDelta)").
        validate(x => if (x >= 1) success else failure("The value should be positive."))
      opt[Double]("prob-for-zero-for-tie-breaking").abbr("pz").action((x, c) => c.copy(probForZeroForTieBreaking = x)).
        text(s"probability to choose zero when several column values (including zero) have the same error delta (default=$defaultProbForZeroForTieBreaking)").
        validate(x => if (x >= 0 && x <= 1) success else failure("value should be between 0 and 1."))
      opt[Double]("max-zero-percentage").abbr("mz").action((x, c) => c.copy(maxZeroPercentage = x)).
        text(s"maximum percentage of zeros in a factor matrix (default=$defaultMaxZeroPercentage)").
        validate(x => if (x >= 0 && x <= 1) success else failure("value should be between 0 and 1."))
      opt[Int]("max-rank-split-size").abbr("mrss").action((x, c) => c.copy(maxRankSplitSize = x)).
        text(s"maximum number of rows out of which to build a single cache table (default=$defaultMaxRankSplitSize).").
        validate(x => if (x >= 1) success else failure("The value should be positive."))
      opt[String]("unfolded-tensor-partitioner").hidden().action((x, c) => c.copy(unfoldedTensorPartitioner = {if (x == "map-based") UNFOLDED_TENSOR_PARTITIONER_MAP_BASED else UNFOLDED_TENSOR_PARTITIONER_ARRAY_BASED}))
        .validate(x => if (Array("map-based", "array-based") contains x) success else failure(s"invalid parameter value: $x"))
        .text("the type of partitioner for unfolded tensor ('map-based' or 'array-based').")
      opt[Int]('p', "num-unfolded-tensor-partitions").abbr("up").required().action((x, c) => c.copy(numUnfoldedTensorPartitions = x)).text("number of partitions of unfolded tensors")
      opt[Int]('i', "num-input-tensor-partitions").abbr("ip").action((x, c) => c.copy(numInputTensorPartitions = x)).text("number of partitions of the input tensor (required for Tucker)")
      opt[String]("compute-error").abbr("ce").action((x, c) => c.copy(computeError = {if (x == "every") COMPUTE_ERROR_FOR_EVERY_ITER else COMPUTE_ERROR_FOR_END_ITER}))
        .validate(x => if (Array("every", "end") contains x) success else failure(s"invalid parameter value: $x"))
        .text("computes the reconstruction error for every iteration (every) or only for the end iteration (end)")
      opt[String]("output-dir-path").abbr("od").action((x, c) => c.copy(outputDirPath = x)).text("path to an output directory (if missing, log messages will be printed only to stdout)")
      opt[String]("output-file-prefix").abbr("op").action((x, c) => c.copy(outputFilePrefix = x)).text("prefix of an output file name")
      opt[String]("tensor-file-separator-type").optional().action((x, c) => c.copy(tensorFileSeparatorType = x)).text(s"separator type of the input tensor file: ssv, tsv, or csv (default: $defaultTensorFileSeparatorType)").
        validate(x => if (Array("ssv", "tsv", "csv").contains(x)) success else failure(s"invalid separator type: $x"))
      opt[Unit]("output-result-summary").hidden().action((_, c) => c.copy(outputResultSummary = true)).text(s"output the best reconstruction error and the running time (secs)")
      checkConfig(c => if (c.outputResultSummary && c.outputDirPath.isEmpty) failure("--output-result-summary requires --output-dir-path to be set") else success)
      checkConfig(c => if (c.factorizationMethod == FACTORIZATION_METHOD_CP && c.rank < 0) failure("--rank should be given for CP factorization") else success)
      checkConfig(c => if (c.factorizationMethod == FACTORIZATION_METHOD_TK && (c.rank1 < 0 || c.rank2 < 0 || c.rank3 < 0)) failure("--rank1, --rank2, and --rank3 should be given for Tucker factorization") else success)
      checkConfig(c => if (c.factorizationMethod == FACTORIZATION_METHOD_TK && c.initialCoreTensorDensity < 0) failure("--initial-core-tensor-density is required for Tucker factorization") else success)
      checkConfig(c => if (c.factorizationMethod == FACTORIZATION_METHOD_TK && c.numInputTensorPartitions < 0) failure("--num-input-tensor-partitions is required for Tucker factorization") else success)
      help("help").text("prints this usage text")
      version("version").text("prints the DBTF version")
      arg[String]("<tensor-file-path>").action((x, c) => c.copy(tensorFilePath = x)).text("input tensor file path")
      note("if --converge-by-num-iters and --converge-by-abs-error-delta are given together, the execution will stop when either of the convergence conditions is satisfied")

      override def showUsageOnError = true
      override def renderingMode: RenderingMode.OneColumn.type = scopt.RenderingMode.OneColumn
    }

    val config: Option[Config] = parser.parse(args, Config())
    if (config.isEmpty) System.exit(1)
    val c = config.get

    val modeLengths = (c.mode1Length, c.mode2Length, c.mode3Length)
    val outputDirPath: Option[String] = if (c.outputDirPath != "") Some(c.outputDirPath) else None
    val outputFilePrefix: Option[String] = if (outputDirPath.isDefined) {
      if (c.outputFilePrefix != "") {
        Some(c.outputFilePrefix)
      } else {
        val _outputFilePrefix = c.tensorFilePath.substring(c.tensorFilePath.lastIndexOf("/") + 1)
        Some(_outputFilePrefix.substring(0, _outputFilePrefix.lastIndexOf(".")))
      }
    } else None

    if (outputDirPath.isDefined) {
      FileUtils.forceMkdir(new File(s"${outputDirPath.get}"))
      logWriter = Some(new PrintWriter(new File(s"${outputDirPath.get}/${outputFilePrefix.get}.log")))
    }

    if (c.outputResultSummary) {
      val resultSummaryFilePrefix = c.tensorFilePath.substring(c.tensorFilePath.lastIndexOf("/") + 1)
      val resultSummaryFileName = resultSummaryFilePrefix.substring(0, resultSummaryFilePrefix.lastIndexOf("."))
      resultSummaryWriter = Some(new PrintWriter(new FileWriter(s"${outputDirPath.get}/$resultSummaryFileName.result", true)))
    }

    log(s"""[Parameters]
        |- tensor file path = ${c.tensorFilePath}
        |- tensor base index = ${c.baseIndex}
        |- rank (for CP) = ${c.rank}
        |- ranks (for Tucker) = (${c.rank1}, ${c.rank2}, ${c.rank3})
        |- randomSeed = ${c.randomSeed}
        |- modeLengths = $modeLengths
        |- numInitialCandidateSets = ${c.numInitialCandidateSets}
        |- initialFactorMatrixDensity = ${c.initialFactorMatrixDensity}
        |- initialCoreTensorDensity = ${c.initialCoreTensorDensity}
        |- convergeByNumIters = ${c.convergeByNumIters} (value=${c.convergeByNumItersValue})
        |- convergeByAbsErrorDelta = ${c.convergeByAbsErrorDelta} (value=${c.convergeByAbsErrorDeltaValue})
        |- numConsecutiveItersForConvergeByAbsErrorDelta = ${c.numConsecutiveItersForConvergeByAbsErrorDelta}
        |- numUnfoldedTensorPartitions = ${c.numUnfoldedTensorPartitions}
        |- numInputTensorPartitions = ${c.numInputTensorPartitions}
        |- computeError = ${c.computeError}
        |- outputDirPath = ${outputDirPath.getOrElse("N/A")}
        |- outputFilePrefix = ${outputFilePrefix.getOrElse("N/A")}
        |- probForZeroForTieBreaking = ${c.probForZeroForTieBreaking}
        |- maxZeroPercentage = ${c.maxZeroPercentage}
        |- maxRankSplitSize = ${c.maxRankSplitSize}
      """.stripMargin)

    /** Assert parameters */
    if (c.factorizationMethod == FACTORIZATION_METHOD_CP) {
      assert(c.rank > 0, c.rank)
    } else {
      assert(c.rank1 > 0 && c.rank2 > 0 && c.rank3 > 0, s"${c.rank1}, ${c.rank2}, ${c.rank3}")
    }

    val ranks: Array[Int] =
      if (c.factorizationMethod == FACTORIZATION_METHOD_CP) {
        Array(c.rank, c.rank, c.rank)
      } else {
        Array(c.rank1, c.rank2, c.rank3)
      }

    for ((x, xi) <- ranks.zipWithIndex; (y, yi) <- ranks.zipWithIndex if yi > xi) {
      assert(!DBTF.checkIntegerOverflowFromProduct(x, y), (x, y))
    }

    val sc = setUpSpark()

    /** Load input tensor */
    val sepRegex = c.tensorFileSeparatorType match {
      case "csv" => ","
      case "ssv" => " "
      case "tsv" => "\t"
    }

    val tensorRDD = TensorOperation.convertToBase0Tensor(TensorOperation.parseTensorCheckingComments(sc, sc.textFile(c.tensorFilePath), sepRegex), c.baseIndex).setName("Raw input tensor")

    val dimA = if (c.factorizationMethod == FACTORIZATION_METHOD_CP) (c.mode1Length, c.rank) else (c.mode1Length, c.rank1)
    val dimB = if (c.factorizationMethod == FACTORIZATION_METHOD_CP) (c.mode2Length, c.rank) else (c.mode2Length, c.rank2)
    val dimC = if (c.factorizationMethod == FACTORIZATION_METHOD_CP) (c.mode3Length, c.rank) else (c.mode3Length, c.rank3)

    log(s"Starting ${c.factorizationMethod} factorization at ${Calendar.getInstance.getTime}.")
    val t0 = System.nanoTime()
    var t1 = 0L

    val convergenceMethod = if (!c.convergeByNumIters && !c.convergeByAbsErrorDelta) {
      DBTF.Convergence.BY_NUM_ITERATION
    } else {
      if (c.convergeByAbsErrorDelta) {
        DBTF.Convergence.BY_ABS_ERROR_DELTA
      } else {
        assert(c.convergeByNumIters)
        DBTF.Convergence.BY_NUM_ITERATION
      }
    }

    val (finalFactorMatrix1, finalFactorMatrix2, finalFactorMatrix3, finalCoreTensor, reconstructionErrors, totalTimeFactors, totalTimeCore) =
      if (c.factorizationMethod == FACTORIZATION_METHOD_CP) {
        /** CP Factorization */
        val ret = DBTF.cpFactorization(
          sc, tensorRDD, modeLengths, c.rank, c.randomSeed, c.unfoldedTensorPartitioner, c.numUnfoldedTensorPartitions,
          c.numInitialCandidateSets, c.initialFactorMatrixDensity, c.convergeByNumIters, c.convergeByNumItersValue,
          c.convergeByAbsErrorDeltaValue, c.numConsecutiveItersForConvergeByAbsErrorDelta,
          convergenceMethod, c.probForZeroForTieBreaking, c.maxZeroPercentage, c.maxRankSplitSize, c.computeError
        )
        (ret._1, ret._2, ret._3, None, ret._4, ret._5, -1L)
      } else {
        /** Tucker Factorization */
        val ret = DBTF.tuckerFactorization(
          sc, tensorRDD, modeLengths, (c.rank1, c.rank2, c.rank3), c.randomSeed, c.unfoldedTensorPartitioner,
          c.numUnfoldedTensorPartitions, c.numInputTensorPartitions,
          c.numInitialCandidateSets, c.initialFactorMatrixDensity, c.initialCoreTensorDensity,
          c.convergeByNumIters, c.convergeByNumItersValue,
          c.convergeByAbsErrorDeltaValue, c.numConsecutiveItersForConvergeByAbsErrorDelta, convergenceMethod,
          c.probForZeroForTieBreaking, c.maxZeroPercentage, c.maxRankSplitSize, c.computeError
        )
        (ret._1, ret._2, ret._3, ret._4, ret._5, ret._6, ret._7)
      }
    t1 = System.nanoTime()
    val elapsedTimeInSec = (t1 - t0) / math.pow(10, 9)
    val totalTimeFactorInSec = totalTimeFactors / math.pow(10, 9)
    val totalTimeCoreInSec = totalTimeCore / math.pow(10, 9)

    log(s"${c.factorizationMethod} factorization ended at ${Calendar.getInstance.getTime}.")
    log(s"Total elapsed time: $elapsedTimeInSec secs")
    log(s"Total elapsed time (updating factors): $totalTimeFactorInSec secs")
    log(s"Total elapsed time (updating a core): $totalTimeCoreInSec secs")

    /** Compute reconstruction error */
    val error =
      if (c.computeError == COMPUTE_ERROR_FOR_END_ITER) {
        Some(DBTF.computeReconstructionError(sc, c.factorizationMethod, tensorRDD, finalFactorMatrix1,
          finalFactorMatrix2, finalFactorMatrix3, dimA, dimB, dimC, finalCoreTensor))
      } else if (c.computeError == COMPUTE_ERROR_FOR_EVERY_ITER) {
        val (minError, minErrorIndex) = reconstructionErrors.zipWithIndex.minBy(_._1)
        log(s"\nMinimum reconstruction error=$minError (number of non-zeros=${tensorRDD.count()}) occurred at Iteration-${minErrorIndex + 1}\n")
        Some(minError)
      } else {
        None
      }

    if (resultSummaryWriter.isDefined) {
      resultSummaryWriter.get.write(s"${if (error.isDefined) error.get else "N/A"}\t$elapsedTimeInSec\t$totalTimeFactorInSec\t$totalTimeCoreInSec\n")
      resultSummaryWriter.get.flush()
    }

    /** Save factor matrices in both dense and sparse forms */
    if (outputDirPath.isDefined) {
      saveFactorMatrices(Array(finalFactorMatrix1, finalFactorMatrix2, finalFactorMatrix3), Array(dimA, dimB, dimC),
        ranks, c.baseIndex, outputDirPath.get, outputFilePrefix.get)

      if (c.factorizationMethod == FACTORIZATION_METHOD_TK) {
        saveCoreTensor(finalCoreTensor.get.toSet, c.baseIndex, outputDirPath.get, outputFilePrefix.get)
      }
    }

    tensorRDD.unpersist()
    logWriter.foreach(_.close())
    resultSummaryWriter.foreach(_.close())
    sc.stop()
  }

  def setUpSpark(): SparkContext = {
    val sparkLogger = Logger.getLogger("org.apache.spark")
    sparkLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("DBTF")
    // use kryo serialization
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrationRequired", "true")
    sparkConf.registerKryoClasses(Array(
      classOf[java.lang.Class[Any]],
      Class.forName("org.apache.spark.rdd.MapPartitionsRDD"),
      Class.forName("org.apache.spark.rdd.ParallelCollectionRDD"),
      Class.forName("org.apache.spark.rdd.RDD$$anonfun$flatMap$1$$anonfun$apply$6"),
      Class.forName("org.apache.spark.SparkContext$$anonfun$hadoopFile$1"),
      Class.forName("org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30"),
      Class.forName("util.TensorOperation$$anonfun$parseTensor$1"),
      Class.forName("org.apache.spark.rdd.HadoopRDD"),
      Class.forName("java.util.Date"),
      Class.forName("org.apache.hadoop.mapred.TextInputFormat"),
      Class.forName("org.apache.spark.broadcast.TorrentBroadcast"),
      Class.forName("org.apache.spark.storage.BroadcastBlockId"),
      Class.forName("org.apache.spark.rdd.RDD$$anonfun$map$1$$anonfun$apply$5"),
      Class.forName("org.apache.spark.rdd.RDD$$anonfun$flatMap$1$$anonfun$apply$6"),
      Class.forName("org.apache.spark.SparkContext$$anonfun$textFile$1$$anonfun$apply$8"),
      Class.forName("util.TensorOperation$$anonfun$convertToBase0Tensor$1"),
      Class.forName("util.TensorOperation$$anonfun$parseTensorCheckingComments$1"),
      Class.forName("org.apache.spark.OneToOneDependency"),
      Class.forName("scala.util.Random$"),
      Class.forName("scala.collection.mutable.WrappedArray$ofRef"),
      Class.forName("scala.collection.mutable.WrappedArray$ofLong"),
      Class.forName("scala.reflect.ClassTag$$anon$1"),
      Class.forName("scala.collection.immutable.HashMap$EmptyHashMap$"),
      //      classOf[scala.collection.MapLike$MappedValues],
      classOf[UnfoldedTensorPartitionRange], classOf[InputTensorPartitionRange],
      classOf[java.util.TreeMap[Long, (Int, (Int, (Int, Int)))]],
      classOf[Array[Int]], classOf[Array[Array[Int]]], classOf[Array[Array[Array[Int]]]],
      classOf[Array[Array[Byte]]],
      classOf[mutable.LongMap[Array[mutable.ArrayBuffer[Int]]]],
      classOf[mutable.LongMap$$anonfun$1],
      classOf[Array[mutable.ArrayBuffer[Int]]],
      classOf[mutable.ArrayBuffer[Int]], classOf[mutable.ArrayBuffer[(Int, Int)]],
      classOf[mutable.ArrayBuffer[(Int, Int, Int)]], classOf[mutable.ArrayBuffer[((Int, Int), Long)]],
      classOf[Array[java.lang.Object]], classOf[Array[java.util.BitSet]],
      //      classOf[dbtf.DBTFUnfoldedTensorPartitioner$$anonfun$partition$1$$anonfun$5$$anonfun$apply$1],
      classOf[dbtf.TensorEntrySet]
    ))
    new SparkContext(sparkConf)
  }

  def saveFactorMatrices(sparseFactorMatrices: Array[Array[(Int, Int)]], dims: Array[(Int, Int)], ranks: Array[Int],
                         baseIndex: Int, outputDirPath: String, outputFilePrefix: String): Unit = {
    assert(sparseFactorMatrices.length == dims.length && dims.length == ranks.length && ranks.length == 3)
    log("Saving factor matrices...")

    /** Save factor matrices in both sparse and dense form */
    for (i <- 1 to 3) {
      val rank = ranks(i - 1)
      val singleBitArray = DBTF.prepareSingleBitArray(bitLength = rank)
      val denseFactorMatrixPath = s"$outputDirPath/${outputFilePrefix}_factor${i}_dense.txt"
      val denseWriter = new PrintWriter(new File(denseFactorMatrixPath))
      val sparseFactorMatrix = sparseFactorMatrices(i - 1)
      MatrixOperation.sparseToBitRowMatrix(sparseFactorMatrix, dims(i - 1), singleBitArray, baseIndex = 0).foreach {
        bitRow => log(DBTF.bitSetToBinaryString(bitRow, rank), Some(denseWriter), printToConsole = false)
      }
      denseWriter.close()

      val sparseFactorMatrixPath = s"$outputDirPath/${outputFilePrefix}_factor${i}_sparse.txt"
      val sparseWriter = new PrintWriter(new File(sparseFactorMatrixPath))
      sparseFactorMatrix.foreach {
        case (rowIndex0, colIndex0) =>
          val rowIndex = rowIndex0 + baseIndex
          val colIndex = colIndex0 + baseIndex
          log(s"$rowIndex\t$colIndex", Some(sparseWriter), printToConsole = false)
      }
      sparseWriter.close()
    }

    log("Done.")
  }

  def saveCoreTensor(coreTensor: Set[(Int, Int, Int)], baseIndex: Int, outputDirPath: String, outputFilePrefix: String): Unit = {
    log(s"Saving a core tensor (number of entries=${coreTensor.size})...")
    val coreTensorPath = s"$outputDirPath/${outputFilePrefix}_core_tensor.txt"
    val coreTensorWriter = new PrintWriter(new File(coreTensorPath))

    coreTensor.foreach {
      entry0: (Int, Int, Int) =>
        val entry = (entry0._1 + baseIndex, entry0._2 + baseIndex, entry0._3 + baseIndex)
        val entryLine = entry.productIterator.mkString("\t")
        log(entryLine, Some(coreTensorWriter), printToConsole = false)
    }
    log("Done.")
  }

  /**
    * Log the given message to the given writer object.
    *
    * @param msg log message
    * @param pw print writer
    * @param printToConsole whether to print to stdout in addition to printing to the writer.
    */
  def log(msg: String, pw: Option[java.io.PrintWriter] = logWriter, printToConsole: Boolean = true): Unit = {
    pw.foreach { w =>
      w.write(s"$msg\n")
      w.flush()
    }
    if (printToConsole) println(msg)
  }
}
