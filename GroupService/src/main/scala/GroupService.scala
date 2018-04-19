package rings

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging

import scala.collection.mutable

sealed trait GroupServerAPI
case class JoinGroup(groupId: BigInt, nodeId: Int)
case class LeaveGroup(groupId: BigInt, nodeId: Int)
case class Message(content: String)
case class JoinAck(groupId: BigInt, success: Boolean)
case class LeaveAck(groupId: BigInt, success: Boolean)

class GroupServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, picker: Picker) extends Actor {
  val cellstore = new KVClient(storeServers)
  val dirtySet = new AnyMap
  val log = Logging(context.system, this)

  var stats = new Stats
  var messagesFromMaster: Int = 0

  var endpoints: Option[Seq[ActorRef]] = None
  var masterActor: Option[ActorRef] = None


  val groupsPerNode = 10
  val numGroups = numNodes * groupsPerNode
  val myGroups = new mutable.HashSet[BigInt]
  val groupsNotIn = new mutable.HashSet[BigInt]

  for (g <- 0 until numGroups) {
    groupsNotIn.add(g)
  }

  def receive = {
    case Prime() =>
      // This will add us to our first group
      masterActor = Some(sender)
      command
    case Command() =>
      messagesFromMaster += 1
      command
    case View(e) =>
      endpoints = Some(e)
    case JoinGroup(g, n) =>
      add(g, n)
    case LeaveGroup(g, n) =>
      remove(g, n)
    case JoinAck(g, s) =>
      if (s) joined(g) else incoming()
    case LeaveAck(g, s) =>
      if (s) left(g) else incoming()
    case Message(c) =>
      stats.groupMessagesReceived += 1
  }

  private def command() = {
    if (endpoints.isDefined) {
      if (myGroups.isEmpty) {
        join(picker.pickGroup(groupsNotIn))
      } else {
        val cmd = picker.pickCommand()
        cmd match {
          case CommandType.Join => if (!groupsNotIn.isEmpty) join(picker.pickGroup(groupsNotIn)) else incoming()
          case CommandType.SendMsg => sendMessage(picker.pickGroup(myGroups))
          case CommandType.Leave => leave(picker.pickGroup(myGroups))
        }
      }
    } else {
      log.warning("endpoints not yet defined")
      incoming()
    }
  }

  private def incoming() = {
    stats.messages += 1
    if (stats.messages >= burstSize) {
      // It is impossible for masterActor to be undefined, since Prime is the first message we receive from master
      masterActor.get ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }

  private def add(groupId: BigInt, memberId: Int): Unit = {
    val memberSet = cellstore.read(groupId).asInstanceOf[Option[mutable.Set[Int]]].getOrElse(new mutable.HashSet[Int])
    if (memberSet.contains(memberId)) sender ! JoinAck(groupId, false) else {
      memberSet.add(memberId)
      cellstore.write(groupId, memberSet, dirtySet)
      cellstore.push(dirtySet)

      // send ack to sender
      sender ! JoinAck(groupId, true)
    }
  }

  private def remove(groupId: BigInt, memberId: Int): Unit = {
    val memberSet = cellstore.read(groupId).asInstanceOf[Option[mutable.Set[Int]]].getOrElse(new mutable.HashSet[Int])
    if (memberSet.contains(memberId)) {
      memberSet.remove(memberId)
      cellstore.write(groupId, memberSet, dirtySet)
      cellstore.push(dirtySet)

      // send ack to sender
      sender ! LeaveAck(groupId, true)
    } else {
      sender ! LeaveAck(groupId, false)
    }
  }

  private def join(groupId: BigInt): Unit = {
    // send a message to group manager with my nodeId
    route(groupId) ! JoinGroup(groupId, myNodeID)
  }

  private def leave(groupId: BigInt): Unit = {
    route(groupId) ! LeaveGroup(groupId, myNodeID)
  }

  private def sendMessage(groupId: BigInt): Unit = {
    // fetch member set
    val content = s"Hello from server $myNodeID"
    for {
      members <- cellstore.directRead(groupId).asInstanceOf[Option[mutable.Set[Int]]]
      m <- members
    } endpoints.get(m) ! Message(content)

    stats.groupMessagesSent += 1
    stats.sentMessageMap.put(groupId, stats.sentMessageMap.get(groupId).getOrElse(0) + 1)
    incoming()
  }

  private def joined(groupId: BigInt): Unit = {
    myGroups.add(groupId)
    groupsNotIn.remove(groupId)
    stats.groupsJoined += 1
    incoming()
  }

  private def left(groupId: BigInt): Unit = {
    myGroups.remove(groupId)
    groupsNotIn.add(groupId)
    stats.groupsLeft += 1
    incoming()
  }

  private def route(key: BigInt): ActorRef = {
    endpoints.get((key % endpoints.get.length).toInt)
  }
}

object GroupServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, picker: Picker): Props = {
    Props(classOf[GroupServer], myNodeID, numNodes, storeServers, burstSize, picker)
  }
}
