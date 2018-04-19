package rings

import scala.collection.mutable

class Stats {
  var messages: Int = 0
  var groupsJoined: Int = 0
  var groupsLeft: Int = 0
  var groupMessagesSent: Int = 0
  var groupMessagesReceived: Int = 0
  var errors: Int = 0
  var sentMessageMap: mutable.Map[BigInt, Int] = new mutable.HashMap[BigInt, Int]

  def += (right: Stats): Stats = {
    messages += right.messages
    groupsJoined += right.groupsJoined
    groupsLeft += right.groupsLeft
    groupMessagesSent += right.groupMessagesSent
    groupMessagesReceived += right.groupMessagesReceived
    errors += right.errors
    sentMessageMap = sentMessageMap ++ right.sentMessageMap.map{ case (k: BigInt, v: Int) => (k, sentMessageMap.get(k).getOrElse(0) + v)}
	  this
  }

  def printSentMessageMap(): String = {
    val sortedMap = sentMessageMap.toSeq.sortBy{ case (k: BigInt, v: Int) => k}

    var str = ""
    for ((k, v) <- sortedMap) {
      str += s"group: $k has $v messages\n"
    }
    str
  }

  override def toString(): String = {
    s"Stats msgsFromMaster=$messages grpsJoined=$groupsJoined grpsLeft=$groupsLeft gMsgsSent=$groupMessagesSent gMsgsRcvd=$groupMessagesReceived err=$errors\n" + printSentMessageMap()
  }
}
