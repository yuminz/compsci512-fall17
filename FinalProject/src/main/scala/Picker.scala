package slicer

import scala.collection.immutable.Set

trait Picker {
  def name(): String
  def nextKey() : Long
}

class UniformPicker (keys: Set[Long]) extends Picker {
  // Lists are faster to iterate (?)
  val keysList = keys.toArray
  var iter = keysList.iterator

  def name() : String = "Uniform Picker for keys " + keys.mkString(", ")

  def nextKey() : Long = {
    if (!iter.hasNext)
      iter = keysList.iterator
    iter.next
  }
}

class RangePicker (firstKey: Long, lastKey: Long) extends Picker {
  var nextV = firstKey - 1

  def name() : String = "Range Picker from " + firstKey + " to " + lastKey

  def nextKey() : Long = {
    if (nextV < lastKey - 1)
      nextV = nextV + 1
    else
      nextV = firstKey
    nextV
  }
}
