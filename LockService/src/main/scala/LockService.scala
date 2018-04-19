package rings
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout

import scala.collection.mutable

sealed trait LockServiceApi
case class FindLockManager(clientID: Int, lock: String) extends LockServiceApi
case class BeginPartitionWith(clients: Set[Int]) extends LockServiceApi
case class EndPartitionWith(clients: Set[Int]) extends LockServiceApi

class LockService(leaseTime: Timeout) extends Actor {
  val log = Logging(context.system, this)

  private val partitionedClients = new mutable.HashSet[Int]()
  private val helperMap: mutable.Map[String, ActorRef] = mutable.HashMap[String, ActorRef]()

  override def receive: Receive = {
    case FindLockManager(clientID, lock) => handleFindLockManager(clientID, lock)
    case BeginPartitionWith(clients) => handleBeginPartitionWith(clients)
    case EndPartitionWith(clients) => handleEndPartitionWith(clients)
  }

  def handleBeginPartitionWith(clients: Set[Int]): Unit = {
    for (c <- clients) {
      partitionedClients.add(c)
    }
    for (h <- helperMap.values) {
      h ! BeginPartitionWith(clients)
    }
  }

  def handleEndPartitionWith(clients: Set[Int]): Unit = {
    for (c <- clients) {
      partitionedClients.remove(c)
    }
    for (h <- helperMap.values) {
      h ! EndPartitionWith(clients)
    }
  }

  def handleFindLockManager(clientID: Int, lock: String): Unit = {
    // look in map to see if the managing actor exists, if not create one
    if (!partitionedClients.contains(clientID)) {
      if (!helperMap.contains(lock)) {
        // create a new actor to manage this lock
        val helper = context.actorOf(LockManager.props(leaseTime), "LockManager" + lock)
        helperMap.put(lock, helper)
      }
      val helper = helperMap.get(lock).get
      sender ! helper
    }
  }
}

object LockService {
  def props(leaseTime: Timeout): Props = {
    Props(classOf[LockService], leaseTime)
  }
}
