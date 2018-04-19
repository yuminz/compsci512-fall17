package rings

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.Logging
import akka.util.Timeout

import scala.collection.mutable

sealed trait KVStoreAPI
case class AcquireRead(transactionId: String, key: BigInt) extends KVStoreAPI
case class SessionLeaseTimeout(transactionId: String, timerId: BigInt) extends KVStoreAPI
case class RenewLease(transactionId: String) extends KVStoreAPI
case class Prepare(transactionId: String, keys: mutable.Set[BigInt]) extends KVStoreAPI
case class CommitTransaction(transactionId: String, updates: scala.collection.Map[BigInt, Any]) extends KVStoreAPI
case class AbortTransaction(transactionId: String) extends KVStoreAPI
case class RemoveFromWaitQueue(transactionId: String, key: BigInt) extends KVStoreAPI
case class AbortEarly(transactionId: String) extends KVStoreAPI
case class UpdatePartitions(clients: Set[Int]) extends KVStoreAPI

/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any. KVStores serve as participants in the 2PC protocol.
 */
class LockInfo {
  var holder: Option[String] = None
  val waitingQueue = new mutable.Queue[String]()
}

class KVStore(leaseTime: Timeout) extends Actor {

  val log = Logging.getLogger(context.system, this)
  import context.dispatcher
  private val store = new scala.collection.mutable.HashMap[BigInt, Any]
  private val lockTable = new scala.collection.mutable.HashMap[BigInt, LockInfo]
  val transactionToLocks = new mutable.HashMap[String, mutable.Set[BigInt]]()

  val transactionToTimer = new mutable.HashMap[String, Cancellable]()
  // While we can cancel a transactionToTimer timer, it is possible that the timer goes off and still sends us the
  // SessionLeaseTimeout even though we cancelled it. To prevent this, we have another map that stores the current valid timerID
  // of the timer associated with that transaction. When a timer goes off, we check that its id is still the valid one.
  // Anytime we want to cancel a timer, we must also increment the valid timer ID so that if the SessionLeaseTimeout message
  // is still delivered, it is ignored.
  val transactionToTimerId = new mutable.HashMap[String, BigInt]()
  val transactionToActorRef = new mutable.HashMap[String, ActorRef]()

  // We use this set to make sure that we don't apply the updates for one commit more than once if we are resent the
  // same CommitTransaction message
  val previouslyCommittedTransactions = new mutable.HashSet[String]()
  // We ignore messages from clients that we are currently partitioned from
  private var partitionedClients = Set[Int]()

  override def receive = {
    case AcquireRead(transactionId, key) => handleAcquireRead(transactionId, key)
    case SessionLeaseTimeout(transactionId, timerId) => handleSessionLeaseTimeout(transactionId, timerId)
    case RenewLease(transactionId) => handleRenewLease(transactionId)
    case Prepare(transactionId, keys) => handlePrepare(transactionId, keys)
    case CommitTransaction(transactionId, updates) => handleCommitTransaction(transactionId, updates)
    case AbortTransaction(transactionId) => handleAbortTransaction(transactionId)
    case RemoveFromWaitQueue(transactionId, key) => handleRemoveFromWaitQueue(transactionId, key)
    case AbortEarly(transactionId) => handleAbortEarly(transactionId)
    case UpdatePartitions(clients) => handleUpdatePartitions(clients)
  }

  def handleUpdatePartitions(clients: Set[Int]): Unit = {
    partitionedClients = clients
  }

  def isClientPartitioned(transactionId: String): Boolean = {
    val strs = transactionId.split("_")
    partitionedClients.contains(Integer.parseInt(strs(0)))
  }

  def handleAcquireRead(transactionId: String, key: BigInt): Unit = {
    if (!isClientPartitioned(transactionId)) {
      var info = lockTable.get(key)
      transactionToActorRef.put(transactionId, sender)
      if (!info.isDefined) {
        info = Some(new LockInfo)
        lockTable.put(key, info.get)
      }

      if (info.get.holder.isDefined) {
        // add to waiting queue
        info.get.waitingQueue.enqueue(transactionId)
      } else {
        info.get.holder = Some(transactionId)
        val transactionInfo = transactionToLocks.get(transactionId)
        if (transactionInfo.isDefined) {
          transactionInfo.get.add(key)
          transactionToTimer.get(transactionId).get.cancel()
          // increment timerId
          transactionToTimerId.put(transactionId, transactionToTimerId.get(transactionId).get + 1)
        } else {
          transactionToLocks.put(transactionId, mutable.Set[BigInt](key))
          transactionToTimerId.put(transactionId, 1)
        }
        // setup lease timer
        val leaseScheduler = context.system.scheduler.scheduleOnce(leaseTime.duration, self, SessionLeaseTimeout(transactionId, transactionToTimerId.get(transactionId).get))
        transactionToTimer.put(transactionId, leaseScheduler)
        sender ! AcquireSucceed(store.get(key), leaseTime.duration)
      }
    }
  }

  def handleSessionLeaseTimeout(transactionId: String, timerId: BigInt): Unit = {
    if (timerId == transactionToTimerId.get(transactionId).get) {
      // free its locks and remove from transactionToTimer
      cleanupTransaction(transactionId)
      transactionToTimer.remove(transactionId)
    }
  }

  def handleRenewLease(transactionId: String): Unit = {
    if (!isClientPartitioned(transactionId)) {
      if (transactionToLocks.contains(transactionId)) {
        val currentLeaseTimer = transactionToTimer.get(transactionId)
        if (currentLeaseTimer.isDefined) {
          currentLeaseTimer.get.cancel()
          transactionToTimerId.put(transactionId, transactionToTimerId.get(transactionId).get + 1)
        }

        val newLeaseTimer = context.system.scheduler.scheduleOnce(leaseTime.duration, self, SessionLeaseTimeout(transactionId, transactionToTimerId.get(transactionId).get))
        transactionToTimer.put(transactionId, newLeaseTimer)
        sender ! LeaseRenewed(leaseTime.duration)
      }
    }
  }

  def handlePrepare(transactionId: String, keys: mutable.Set[BigInt]): Unit = {
    if (!isClientPartitioned(transactionId)) {
      val locksHeld = transactionToLocks.get(transactionId)
      if (locksHeld.isDefined && locksHeld.get.equals(keys)) {
        sender ! VoteCommit
      } else {
        sender ! VoteAbort
        // release locks right away
        cleanupTransaction(transactionId)
      }
      // cancel session lease timer for this transaction regardless of vote
      val sessionLeaseTimer = transactionToTimer.get(transactionId)
      if (sessionLeaseTimer.isDefined) {
        sessionLeaseTimer.get.cancel()
        transactionToTimerId.put(transactionId, transactionToTimerId.get(transactionId).get + 1)
      }
      transactionToTimer.remove(transactionId)
    }
  }

  def handleCommitTransaction(transactionId: String, updates: scala.collection.Map[BigInt, Any]): Unit = {
    if (!isClientPartitioned(transactionId)) {
      if (!previouslyCommittedTransactions.contains(transactionId)) {
        for ((key, value) <- updates) {
          store.put(key, value)
        }
        previouslyCommittedTransactions.add(transactionId)
        cleanupTransaction(transactionId)
      }
      sender ! Acknowledgement
    }
  }

  def handleAbortTransaction(transactionId: String): Unit = {
    if (!isClientPartitioned(transactionId)) {
      handleAbortEarly(transactionId)
      sender ! Acknowledgement
    }
  }

  def handleRemoveFromWaitQueue(transactionId: String, key: BigInt): Unit = {
    if (!isClientPartitioned(transactionId)) {
      val lockInfo = lockTable.get(key)
      if (lockInfo.isDefined) {
        val waitingQueue = lockInfo.get.waitingQueue
        if (waitingQueue.contains(transactionId)) {
          waitingQueue.dequeueFirst((s: String) => s.equals(transactionId))
        }
      }
    }
  }

  def handleAbortEarly(transactionId: String): Unit = {
    if (!isClientPartitioned(transactionId)) {
      cleanupTransaction(transactionId)
      val sessionLeaseTimer = transactionToTimer.get(transactionId)
      if (sessionLeaseTimer.isDefined) {
        sessionLeaseTimer.get.cancel()
        transactionToTimerId.put(transactionId, transactionToTimerId.get(transactionId).get + 1)
      }
      transactionToTimer.remove(transactionId)
    }
  }

  def cleanupTransaction(transactionId: String): Unit = {
    val locksHeld = transactionToLocks.get(transactionId).get
    for (lock <- locksHeld) {
      // free it and grant to next person if possible
      val lockInfo = lockTable.get(lock).get
      lockInfo.holder = None
      if (lockInfo.waitingQueue.size > 0) {
        val newHolder = lockInfo.waitingQueue.dequeue()
        lockInfo.holder = Some(newHolder)
        val transactionInfo = transactionToLocks.get(newHolder)
        if (transactionInfo.isDefined) {
          transactionInfo.get.add(lock)
          transactionToTimer.get(newHolder).get.cancel()
          transactionToTimerId.put(newHolder, transactionToTimerId.get(newHolder).get + 1)
        } else {
          transactionToLocks.put(newHolder, mutable.Set[BigInt](lock))
          transactionToTimerId.put(newHolder, 1)
        }
        // setup lease timer
        val leaseScheduler = context.system.scheduler.scheduleOnce(leaseTime.duration, self, SessionLeaseTimeout(newHolder, transactionToTimerId.get(transactionId).get))
        transactionToTimer.put(newHolder, leaseScheduler)
        if (!isClientPartitioned(newHolder)) {
          transactionToActorRef.get(newHolder).get ! AcquireSucceed(store.get(lock), leaseTime)
        }
      }
    }
    transactionToLocks.remove(transactionId)
  }
}

object KVStore {
  def props(leaseTime: Timeout): Props = {
     Props(classOf[KVStore], leaseTime)
  }
}