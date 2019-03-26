/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package dbtf

import org.apache.spark.Partitioner

/**
  * Maps the key of RDD to its partition ID (0 to numPartitions - 1).
  *
  * @param numParts number of partitions
  */
class IdentityPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val partitionId = key.toString.toInt
    assert(partitionId >= 0 && partitionId < numPartitions)
    partitionId
  }

  override def equals(other: Any): Boolean = other match {
    case p: IdentityPartitioner => numPartitions == p.numPartitions
    case _ => false
  }
}
