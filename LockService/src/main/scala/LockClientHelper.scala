package rings


import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await

sealed trait LockClientHelperApi
case class AcquireLock(waitTime: Timeout) extends LockClientHelperApi
case object Denied extends LockClientHelperApi
case object Release extends LockClientHelperApi
case class Granted(leaseTime: Timeout) extends LockClientHelperApi
case class RenewTimeout(leaseTime: Timeout) extends LockClientHelperApi
case object AcquireTimeout extends LockClientHelperApi
case object GetFinalStats extends LockClientHelperApi
case object RecallRequest extends LockClientHelperApi

class LockStats {
  var numCachedAcquires = 0
  var numAcquireWithNetwork = 0

  def += (right: LockStats): LockStats = {
    numCachedAcquires += right.numCachedAcquires
    numAcquireWithNetwork += right.numAcquireWithNetwork
    this
  }

  override def toString(): String = {
    "Percentage of lock acquires with the lock already cached is: " + numCachedAcquires.asInstanceOf[Float] / (numAcquireWithNetwork + numCachedAcquires) * 100 + "%"
  }
}

class LockClientHelper(val myNodeID: Int, manager: ActorRef, client: ActorRef, lock: String) extends Actor {
  val log = Logging(context.system, this)
  import context.dispatcher

  var holdsLock = false
  var activelyUsed = false
  var renewScheduler: Cancellable = null

  var pendingLockRequest = false
  var clientWaiting: ActorRef = null
  var acquireScheduler: Cancellable = null

  val myStats = new LockStats


  def receive = {
    case AcquireLock(t) => handleAcquire(t)
    case Release => handleRelease()
    case Granted(leaseTime) => handleGranted(leaseTime)
    case RenewTimeout(leaseTime: Timeout) => handleRenew(leaseTime)
    case AcquireTimeout => handleAcquireTimeout()
    case RecallRequest => handleRecallRequest()
    case GetFinalStats => {
      sender ! myStats
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    if (renewScheduler != null) {
      renewScheduler.cancel()
      renewScheduler = null
    }
  }

  def handleRecallRequest(): Unit = {
    if (activelyUsed) {
      sender ! RecallDenied(myNodeID)
    } else {
      holdsLock = false
      if (renewScheduler != null) {
        renewScheduler.cancel()
        renewScheduler = null
      }
      sender ! RecallAccepted(myNodeID)
    }
  }

  def handleAcquireTimeout(): Unit = {
    clientWaiting ! false
    clientWaiting = null
  }

  def handleGranted(leaseTime: Timeout): Unit = {
    if (acquireScheduler != null) {
      acquireScheduler.cancel()
      acquireScheduler = null
    }
    holdsLock = true
    activelyUsed = true
    pendingLockRequest = false
    renewScheduler = context.system.scheduler.scheduleOnce(leaseTime.duration / 2, self, RenewTimeout(leaseTime))
    if (clientWaiting != null) {
      clientWaiting ! true
    }
  }

  def handleAcquire(waitTime: Timeout): Unit = {
    if (holdsLock) {
      // lock is already cached so we don't need to send a message to the lock server
      myStats.numCachedAcquires += 1
      activelyUsed = true
      sender ! true
    } else {
      myStats.numAcquireWithNetwork += 1
      if (pendingLockRequest) {
        clientWaiting = sender
        acquireScheduler = context.system.scheduler.scheduleOnce(waitTime.duration, self, AcquireTimeout)
      } else {
        manager ! Acquire(myNodeID)
        clientWaiting = sender
        acquireScheduler = context.system.scheduler.scheduleOnce(waitTime.duration, self, AcquireTimeout)
        pendingLockRequest = true
      }
    }
  }

  def handleRenew(leaseTime: Timeout): Unit = {
    // if timeout expires for renew lease
    try {
      val future = ask(manager, Renew(myNodeID, activelyUsed))(leaseTime.duration / 4)
      val result = Await.result(future, leaseTime.duration / 4)
      if (result.isInstanceOf[Granted]) {
        val lease = result.asInstanceOf[Granted]
        renewScheduler = context.system.scheduler.scheduleOnce(lease.leaseTime.duration / 2, self, RenewTimeout(lease.leaseTime))
      } else {
        // we would only receive a denied if the lock isn't activelyUsed, so we don't have to send LockLost to holder
        holdsLock = false
      }
    } catch {
      case e: TimeoutException => {
        // we no longer hold the lock
        if (activelyUsed) {
          client ! LockLost(lock)
        }
        holdsLock = false
        activelyUsed = false
      }
    }
  }

  def handleRelease(): Unit = {
    activelyUsed = false
  }
}


object LockClientHelper {
  def props(myNodeID: Int, manager: ActorRef, client: ActorRef, lock: String): Props = {
    Props(classOf[LockClientHelper], myNodeID, manager, client, lock)
  }
}
