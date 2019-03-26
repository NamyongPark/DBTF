/***********************************************************************
 Fast and Scalable Method for Distributed Boolean Tensor Factorization

 Authors: Namyong Park (namyongp@cs.cmu.edu), Sejoon Oh (sejun6431@gmail.com), U Kang (ukang@snu.ac.kr)
 Data Mining Lab., Seoul National University

 Version: 2.0

 This software is free of charge under research purposes.
 For commercial purposes, please contact the authors.
***********************************************************************/

package dbtf

import scala.collection.mutable

class TensorEntrySet(tensorEntries: Iterable[(Int, Int, Int)]) extends Serializable {
  private val map: mutable.LongMap[mutable.LongMap[mutable.Set[Int]]] = mutable.LongMap.empty[mutable.LongMap[mutable.Set[Int]]]

  private def buildMap(): Unit = {
    val groupedEntries: Map[Int, Map[Int, mutable.Set[Int]]] = tensorEntries.groupBy { entry => entry._1 }.mapValues {
      entries1: Iterable[(Int, Int, Int)] =>
        entries1.map(entry1 => (entry1._2, entry1._3)).groupBy { e => e._1 }.mapValues {
          entries2: Iterable[(Int, Int)] =>
            mutable.Set(entries2.map(entry2 => entry2._2).toArray: _*)
        }
    }

    groupedEntries.foreach {
      case (dim1, groupedEntries23) =>
        val subMap = mutable.LongMap.empty[mutable.Set[Int]]
        groupedEntries23.foreach {
          case (dim2, entries3) =>
            assert(!subMap.contains(dim2), s"$dim2 already exists.")
            subMap.put(dim2, entries3)
        }

        assert(!map.contains(dim1), s"$dim1 already exists.")
        map.put(dim1, subMap)
    }
  }

  buildMap()

  def contains(entry: (Int, Int, Int)): Boolean = {
    val _subMap = map.get(entry._1)
    if (_subMap.isEmpty) {
      false
    } else {
      val _subSubSet = _subMap.get.get(entry._2)
      if (_subSubSet.isEmpty) {
        false
      } else {
        _subSubSet.get.contains(entry._3)
      }
    }
  }
  def getSubMap(iIndex: Int): Option[mutable.LongMap[mutable.Set[Int]]] = {
    map.get(iIndex)
  }

  def add(entry: (Int, Int, Int)): Boolean = {
    val _subMap = map.get(entry._1)
    if (_subMap.isEmpty) {
      val newSubMap = mutable.LongMap[mutable.Set[Int]]((entry._2, mutable.Set[Int](entry._3)))
      val ret = map.put(entry._1, newSubMap)
      ret.isEmpty
    } else {
      val subMap = _subMap.get
      val _subSubSet = subMap.get(entry._2)
      if (_subSubSet.isEmpty) {
        val ret = subMap.put(entry._2, mutable.Set(entry._3))
        ret.isEmpty
      } else {
        val subSubSet = _subSubSet.get
        val ret = subSubSet.add(entry._3)
        ret
      }
    }
  }

  def remove(entry: (Int, Int, Int)): Boolean = {
    val _subMap = map.get(entry._1)
    if (_subMap.isEmpty) {
      false
    } else {
      val subMap = _subMap.get
      val _subSubSet = subMap.get(entry._2)
      if (_subSubSet.isEmpty) {
        false
      } else {
        val subSubSet = _subSubSet.get
        subSubSet.remove(entry._3)
      }
    }
  }

  def toArray: Array[(Int, Int, Int)] = {
    map.flatMap {
      case (dim1, entries23) =>
        entries23.flatMap {
          case (dim2, entries3) =>
            entries3.map(dim3 => (dim1.toInt, dim2.toInt, dim3))
        }
    }.toArray
  }
}