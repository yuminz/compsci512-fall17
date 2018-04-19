package rings
import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.Logging
import akka.util.Timeout
import akka.pattern.ask

import scala.collection.mutable
import scala.concurrent.TimeoutException
import scala.concurrent.duration._

sealed trait LockManagerApi
case class Acquire(myNodeID: Int) extends LockManagerApi
case class Renew(myNodeID: Int, activelyUsed: Boolean) extends LockManagerApi
case object GrantToClient extends LockManagerApi
case object RecallTimeout extends LockManagerApi
// We use this message for testing purposes
case object GetLockInfo extends LockManagerApi

sealed trait RecallResponse
case class RecallAccepted(clientID: Int) extends RecallResponse
case class RecallDenied(clientID: Int) extends RecallResponse

class LockManager(leaseTime: Timeout) extends Actor {
  val log = Logging(context.system, this)

  var currentlyHeld: Boolean = false
  // this is the actual actor holding the lock, we respond to this on an acquire and when we grant a lock to a new actor
  var currentHolder: ActorRef = null
  var currentHolderID: Option[Int] = None
  val waitingQueue = mutable.Queue[Tuple2[ActorRef, Int]]()

  // this scheduler goes off when the current lease has expired
  var renewCheck: Option[Cancellable] = None
  // this timer goes off when it is time to send another recall request message to current holder
  var recallScheduler: Option[Cancellable] = None
  var recallScheduleMade: Boolean = false
  // after denying a lease renewal request from the current lock holder, we must block half of the lease term before granting
  // the lock to the next client, so that two clients don't hold the same lock at once
  var grantToClientScheduler: Option[Cancellable] = None

  import context.dispatcher

  private val partitionedClients = new mutable.HashSet[Int]()

  private val recallRequestTime = Timeout(leaseTime.duration)

  def receive = {
    case Acquire(clientID) => handleAcquire(clientID)
    case Renew(clientID, used) => handleRenew(clientID, used)
    case RenewTimeout => handleRenewCheck()
    case BeginPartitionWith(clients) => handleBeginPartitionWith(clients)
    case EndPartitionWith(clients) => handleEndPartitionWith(clients)
    case GrantToClient => handleGrantToClient()
    case RecallTimeout => handleRecallTimeout()
    case GetLockInfo => {
      sender ! currentHolderID
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    if (renewCheck.isDefined) {
      renewCheck.get.cancel()
      renewCheck = None
    }
    if (grantToClientScheduler.isDefined) {
      grantToClientScheduler.get.cancel()
      grantToClientScheduler = None
    }
    if (recallScheduler.isDefined) {
      recallScheduler.get.cancel()
      recallScheduler = None
    }
  }

  def handleBeginPartitionWith(clients: Set[Int]): Unit = {
    for (c <- clients) {
      partitionedClients.add(c)
    }
  }

  def handleEndPartitionWith(clients: Set[Int]): Unit = {
    for (c <- clients) {
      partitionedClients.remove(c)
    }
  }

  def handleRecallTimeout(): Unit = {
    // we don't have to start a new sequence of recall messages if someone else was already on the waiting queue
    // because they have already been started
    if (currentHolderID.isDefined && !partitionedClients.contains(currentHolderID.get)) {
      val future = ask(currentHolder, RecallRequest)(recallRequestTime.duration).mapTo[RecallResponse]
      future.onSuccess {
        case RecallAccepted(clientID) => {
          if (currentHolderID.isDefined && currentHolderID.get == clientID) {
            // we check that we are not partitioned from the current holder, otherwise we would never actually receive this message
            if (!partitionedClients.contains(currentHolderID.get)) {
              // grant lock to next person on queue
              if (renewCheck.isDefined) {
                renewCheck.get.cancel()
                renewCheck = None
              }
              if (grantToClientScheduler.isDefined) {
                grantToClientScheduler.get.cancel()
                grantToClientScheduler = None
              }
              handleGrantToClient()
            } else {
              // if we are partitioned from lock holder just schedule another recall timer
              recallScheduler = Some(context.system.scheduler.scheduleOnce(recallRequestTime.duration, self, RecallTimeout))
            }
          }
        }
        case RecallDenied(clientID) => {
          // lock is not released and we should schedule a new recall
          if (currentHolderID.isDefined && currentHolderID.get == clientID) {
            recallScheduler = Some(context.system.scheduler.scheduleOnce(recallRequestTime.duration, self, RecallTimeout))
          }
        }
      }
      future.onFailure {
        case e: TimeoutException => {
          if (context != null) {
            recallScheduler = Some(context.system.scheduler.scheduleOnce(recallRequestTime.duration, self, RecallTimeout))
          }
        }
      }
    } else {
      recallScheduler = Some(context.system.scheduler.scheduleOnce(recallRequestTime.duration, self, RecallTimeout))
    }
  }

  def handleAcquire(clientID: Int) = {
    if (!partitionedClients.contains(clientID)) {
      if (currentlyHeld) {
        waitingQueue.enqueue((sender, clientID))
        // we don't have to start a new sequence of recall messages if someone else was already on the waiting queue because they have already been started
        if (!recallScheduleMade) {
          // send recall message to currentHolder
          recallScheduleMade = true
          handleRecallTimeout()
        }
      } else {
        currentlyHeld = true
        currentHolder = sender
        currentHolderID = Some(clientID)
        sender ! Granted(leaseTime)
        renewCheck = Some(context.system.scheduler.scheduleOnce(leaseTime.duration, self, RenewTimeout))
      }
    }
  }

  // If this is called, it means the lease hasn't been renewed
  def handleRenewCheck(): Unit = {
    // assume current holder has lost it
    if (waitingQueue.isEmpty) {
      currentHolder = null
      currentHolderID = None
      currentlyHeld = false
      if (recallScheduler.isDefined) {
        recallScheduler.get.cancel()
        recallScheduler = None
      }
    } else {
      if (recallScheduler.isDefined) {
        recallScheduler.get.cancel()
        recallScheduler = None
      }
      // cancel grant to client scheduler
      handleGrantToClient()
    }
  }

  def handleRenew(clientID: Int, activelyUsed: Boolean): Unit = {
    if (!partitionedClients.contains(clientID)) {
      // make sure that sender is actually the one that currently holds the lock
      if (currentHolderID.isDefined && clientID == currentHolderID.get) {
        if (activelyUsed || waitingQueue.isEmpty) {
          sender ! Granted(leaseTime)
          if (renewCheck.isDefined) {
            renewCheck.get.cancel()
            renewCheck = None
          }
          renewCheck = Some(context.system.scheduler.scheduleOnce(leaseTime.duration, self, RenewTimeout))
        } else {
          // we don't want to renew lease
          sender ! Denied
          if (renewCheck.isDefined) {
            renewCheck.get.cancel()
            renewCheck = None
          }
          if (recallScheduler.isDefined) {
            recallScheduler.get.cancel()
            recallScheduler = None
          }
          // give to next person on list
          grantToClientScheduler = Some(context.system.scheduler.scheduleOnce(leaseTime.duration / 2, self, GrantToClient))
        }
      } else {
        sender ! Denied
      }
    }
  }

  def handleGrantToClient(): Unit = {
    val tup = waitingQueue.dequeue()
    currentHolder = tup._1
    currentHolderID = Some(tup._2)
    // don't grant lock to this person if we are partitioned from them
    if (currentHolderID.isDefined && !partitionedClients.contains(currentHolderID.get)) {
      currentHolder ! Granted(leaseTime)
    }
    renewCheck = Some(context.system.scheduler.scheduleOnce(leaseTime.duration, self, RenewTimeout))

    // if others waiting schedule another recall timer
    if (waitingQueue.nonEmpty) {
      recallScheduleMade = true
      recallScheduler = Some(context.system.scheduler.scheduleOnce(recallRequestTime.duration, self, RecallTimeout))
    } else {
      recallScheduleMade = false
    }
  }

}

object LockManager {
  def props(leaseTime: Timeout): Props = {
    Props(classOf[LockManager], leaseTime)
  }
}


