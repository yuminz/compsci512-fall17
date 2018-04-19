package rings

import scala.collection.mutable

class Stats {
  var messages: Int = 0
  var locksAcquired: Int = 0
  var locksReleased: Int = 0
  var errorAcquiringLock: Int = 0
  var locksLost: Int = 0
  var errors: Int = 0
  var lockStats: LockStats = new LockStats
  //var sentMessageMap: mutable.Map[BigInt, Int] = new mutable.HashMap[BigInt, Int]

  def += (right: Stats): Stats = {
    messages += right.messages
    locksAcquired += right.locksAcquired
    locksReleased += right.locksReleased
    errorAcquiringLock += right.errorAcquiringLock
    locksLost += right.locksLost
    errors += right.errors
    lockStats += right.lockStats
    //sentMessageMap = sentMessageMap ++ right.sentMessageMap.map{ case (k: BigInt, v: Int) => (k, sentMessageMap.get(k).getOrElse(0) + v)}
	  this
  }
/*
  def printSentMessageMap(): String = {
    val sortedMap = sentMessageMap.toSeq.sortBy{ case (k: BigInt, v: Int) => k}

    var str = ""
    for ((k, v) <- sortedMap) {
      str += s"group: $k has $v messages\n"
    }
    str
  }*/

  override def toString(): String = {
    s"Stats msgsFromMaster=$messages locksAcquired=$locksAcquired locksReleased=$locksReleased errorAcquiringLock=$errorAcquiringLock locksLost=$locksLost err=$errors lockStats=$lockStats\n"
  }
}
