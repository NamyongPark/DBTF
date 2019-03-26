/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package dbtf

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Partitioner class for input tensor
  */
object InputTensorPartitioner {
  def partitionInputTensor(inputTensorRDD: RDD[(Int, Int, Int)],
                           brPartitionRange: Broadcast[InputTensorPartitionRange]): RDD[(Int, TensorEntrySet)] = {
    val partitioned: RDD[(Int, Iterable[(Int, Int, Int)])] = inputTensorRDD map {
      index =>
        val partitionRange = brPartitionRange.value
        val partitionId = partitionRange.getPartitionId(index)
        (partitionId, index)
    } groupByKey new IdentityPartitioner(brPartitionRange.value.numInputTensorPartitions)

    partitioned.mapValues {
      inputTensorEntries: Iterable[(Int, Int, Int)] =>
        new TensorEntrySet(inputTensorEntries)
    }
  }
}
