package rings
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout

import scala.collection.mutable

sealed trait LockingApplicationApi
case object SendStats extends LockingApplicationApi
case class LockLost(lock: String) extends LockingApplicationApi

// allLocks represents the current set of files on the server
class LockingApplication(myNodeID: Int, lockService: ActorRef, numLocks: Int, picker: Picker, burstSize: Int, lockWaitTime: Timeout) extends Actor {
  val log = Logging(context.system, this)
  val lockClient = new LockClient(myNodeID, lockService, this, lockWaitTime)
  var stats = new Stats
  var messagesFromMaster: Int = 0

  val locksNotYetHeld = new mutable.HashSet[String]()
  val locksHeld = new mutable.HashSet[String]()

  for (i <- 0 until numLocks) {
    locksNotYetHeld.add(i.toString)
  }


  override def receive = {
    case Prime() => {}
    case Command() =>
      messagesFromMaster += 1
      command()
    case LockLost(lock) => handleLockLost(lock)
    case SendStats => {
      stats.lockStats = lockClient.getStats
      sender ! stats
    }
  }

  private def incoming() = {
    stats.messages += 1
    if (stats.messages >= burstSize) {
      // It is impossible for masterActor to be undefined, since Prime is the first message we receive from master
      sender ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }

  private def acquireLock(): Unit = {
    if (locksNotYetHeld.nonEmpty) {
      val lock = picker.pickLock(locksNotYetHeld)
      if (lockClient.acquire(lock)) {
        stats.locksAcquired += 1
        locksHeld.add(lock)
        locksNotYetHeld.remove(lock)
      } else {
        stats.errorAcquiringLock += 1
      }
    }
  }

  private def releaseLock(): Unit = {
    if (locksHeld.nonEmpty) {
      val lock = picker.pickLock(locksHeld)
      lockClient.release(lock)
      stats.locksReleased += 1
      locksHeld.remove(lock)
      locksNotYetHeld.add(lock)
    }
  }

  private def command(): Unit = {
    // use picker
    if (locksHeld.isEmpty) {
      acquireLock()
    } else if (locksNotYetHeld.isEmpty) {
      releaseLock()
    } else {
      val cmd = picker.pickCommand()
      cmd match {
        case CommandType.Acquire => {
          acquireLock()
        }
        case CommandType.Release => {
          releaseLock()
        }
      }
    }
    incoming()
  }

  private def handleLockLost(str: String): Unit = {
    stats.locksLost += 1
    // remove it from held locks
    locksHeld.remove(str)
    locksNotYetHeld.add(str)
  }
}

object LockingApplication {
  def props(myNodeID: Int, lockService: ActorRef, numLocks: Int, picker: Picker, burstSize: Int, lockWaitTime: Timeout): Props = {
    Props(classOf[LockingApplication], myNodeID, lockService, numLocks, picker, burstSize, lockWaitTime)
  }
}
