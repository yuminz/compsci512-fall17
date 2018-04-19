package rings

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.Logging
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.duration._

trait KVClientHelperApi
case class Read(key: BigInt) extends KVClientHelperApi
case class Write(key: BigInt) extends KVClientHelperApi
case class Commit(cache: AnyMap) extends KVClientHelperApi
case object Abort extends KVClientHelperApi
case object AcquireTimeout extends KVClientHelperApi
case class RenewTimeout(store: ActorRef) extends KVClientHelperApi
case class AcquireSucceed(currentValue: Option[Any], leaseTime: Timeout) extends KVClientHelperApi
case object VotingTimeout extends KVClientHelperApi
case object ResendAbortTimeout extends KVClientHelperApi
case object ResendCommitTimeout extends KVClientHelperApi
case object Acknowledgement extends KVClientHelperApi
case object VoteCommit extends KVClientHelperApi
case object VoteAbort extends KVClientHelperApi
case class LeaseTimeout(store: ActorRef, leaseTime: Timeout, timerId: BigInt) extends KVClientHelperApi
case class LeaseRenewed(leaseTime: Timeout) extends KVClientHelperApi


/**
  * KVClientHelper is an actor that is created per transaction of a given client. It manages the acquisition/renewal of
  * leases on keys. It also functions as the coordinator of the 2PC protocol. It is necessary for the client helper
  * to run in a separate actor from the app/KVClient so that the app can block trying to acquire a lock while the
  * clientHelper stays unblocked and can continue to renew its other leases.
  * When we hold more than one lock on keys from a given KVStore, we only have to renew a single session lease
  * with that store - similar to Chubby.
  *
  * @param transactionId each transaction is assigned a unique ID used when interacting with the KVStores
  * @param stores the sequence of KVStore actors
  * @param acquireWaitTime how long the Client is blocking while trying to acquire the lock on a key
  */
class KVClientHelper(transactionId: String, stores: Seq[ActorRef], acquireWaitTime: Timeout) extends Actor {
  import context.dispatcher

  val log = Logging.getLogger(context.system, this)

  // How often we should resend commit/abort messages as long as we don't receive acknowledgements
  val resendCommitTime = Timeout(1 seconds)

  // We have one session lease for each server that we hold at lease one lock with.
  val sessionLeases: mutable.Map[ActorRef, Cancellable] = new mutable.HashMap[ActorRef, Cancellable]()
  // While we can cancel a sessionLease renew timer, it is possible that the timer goes off and still sends us the
  // LeaseTimeout even though we cancelled it. To prevent this, we have another map that stores the current valid timerID
  // of the timer associated with that store. When a timer goes off, we check that its id is still the valid one.
  // Anytime we want to cancel a timer, we must also increment the valid timer ID so that if the LeaseTimeout message
  // is still delivered, it is ignored.
  val sessionLeasesId = new mutable.HashMap[ActorRef, BigInt]()

  // Once we send a RenewLease message to a given store, we schedule a timer to go off if we don't get a LeaseRenewed
  // message in a certain amount of time.
  val renewLeaseTimers: mutable.Map[ActorRef, Cancellable] = new mutable.HashMap[ActorRef, Cancellable]()

  // has aborted is true when we abort the transaction early
  var hasAborted = false
  // true if we have already sent out prepare messages to the stores
  var hasSentPrepares = false

  // the temporary actor in KVClient from ask() that is currently waiting on a response for the read/write calls
  var acquireSender: Option[ActorRef] = None
  // the store server we are waiting to acquire a lock with
  var pendingAcquireWith: Option[ActorRef] = None
  // the key we are waiting to acquire a lock for
  var pendingAcquireFor: Option[BigInt] = None
  // this timer goes off if we can't acquire the lock within a certain amount of time, at which point we should abort
  // the transaction
  var acquireScheduler: Option[Cancellable] = None

  // the temporary actor in KVClient from ask() that is currently waiting on a response for the commit call
  var commitSender: Option[ActorRef] = None
  // this timer goes off if we don't receive all votes from stores in time, at which point we should abort the transaction
  var votingScheduler: Option[Cancellable] = None
  val votesPendingFrom = new mutable.HashSet[ActorRef]()
  var abortReceived = false

  // the set of actors from which we haven't yet received an acknowledgement, we will continue to periodically resend
  // commit/abort messages to this set until it is empty
  val acksPendingFrom = new mutable.HashSet[ActorRef]()

  // these timers go off when we should resend the commit/abort transaction messages to the actors who still
  // haven't sent back an acknowledgement
  var resendAbortScheduler: Option[Cancellable] = None
  var resendCommitScheduler: Option[Cancellable] = None

  // keeps track of the keys from a given store for which we hold locks
  val storeToKeys = new mutable.HashMap[ActorRef, mutable.Set[BigInt]]()
  // passed into KVClientHelper from KVClient at commit time
  var cache: Option[mutable.Map[BigInt, Any]] = None

  override def receive: Receive = {
    case Read(key) => handleRead(key)
    case Write(key) => handleRead(key)
    case Commit(cache) => handleCommit(cache)
    case Abort => abortEarly()
    case AcquireTimeout => handleAcquireTimeout()
    case LeaseTimeout(store, leaseTime, timerId) => handleLeaseTimeout(store, leaseTime, timerId)
    case RenewTimeout(store) => handleRenewTimeout(store)
    case VotingTimeout => handleVotingTimeout()
    case ResendAbortTimeout => handleResendAbortTimeout()
    case ResendCommitTimeout => handleResendCommitTimeout()
    case AcquireSucceed(currentValue, leaseTime) => handleAcquireSucceed(currentValue, leaseTime)
    case VoteCommit => handleVoteCommit()
    case VoteAbort => handleVoteAbort()
    case Acknowledgement => handleAcknowledgement()
    case LeaseRenewed(leaseTime) => handleLeaseRenewed(leaseTime)
  }

  // Because we have exclusive locks on each key, and not reader/writer locks, we treat the read and write procedures the same
  // in the KVClientHelper and KVStore
  def handleRead(key: BigInt): Unit = {
    if (hasAborted) {
      sender ! TransactionAbortedEarly
    } else {
      val store = route(key)
      store ! AcquireRead(transactionId, key)
      // set timer for acquire timeout
      pendingAcquireFor = Some(key)
      pendingAcquireWith = Some(store)
      acquireSender = Some(sender)
      acquireScheduler = Some(context.system.scheduler.scheduleOnce(acquireWaitTime.duration, self, AcquireTimeout))
    }
  }

  def handleAcquireTimeout(): Unit = {
    if (acquireScheduler.isDefined) {
      acquireScheduler = None
      abortEarly()
      // send failed message to client
      acquireSender.get ! AcquireFailed
    }
  }

  def handleAcquireSucceed(value: Option[Any], timeout: Timeout): Unit = {
    if (!hasAborted) {
      val currentLocksWithStore = storeToKeys.get(sender)
      if (currentLocksWithStore.isDefined) {
        currentLocksWithStore.get.add(pendingAcquireFor.get)
      } else {
        storeToKeys.put(sender, mutable.Set(pendingAcquireFor.get))
      }

      if (acquireScheduler.isDefined) {
        // it is a problem if we don't cancel this one correctly, it will appear as if we didn't acquire the lock on time -> will get dead letters
        // that is why we set acquireScheduler = None and check if it is defined in handleAcquireTimeout
        acquireScheduler.get.cancel()
        acquireScheduler = None
      }
      pendingAcquireWith = None
      pendingAcquireFor = None
      // send succeed message with value to app
      acquireSender.get ! AcquireSuccess(value)
      // schedule lease renewal timer
      val oldScheduler = sessionLeases.get(sender)
      if (oldScheduler.isDefined) {
        oldScheduler.get.cancel()
        // we must increment the sessionLease ID so that if the timer has already gone off and the LeaseTimeout message
        // is already delivered, it will be ignored
        sessionLeasesId.put(sender, sessionLeasesId.get(sender).get + 1)
      } else {
        sessionLeasesId.put(sender, 1)
      }
      // We create a new timer to go off after half of the lease has passed so we can renew it
      val renewScheduler = context.system.scheduler.scheduleOnce(timeout.duration / 2, self, LeaseTimeout(sender, timeout, sessionLeasesId.get(sender).get))
      sessionLeases.put(sender, renewScheduler)
    }
  }

  def handleLeaseTimeout(store: ActorRef, leaseTime: Timeout, timerId: BigInt): Unit = {
    if (timerId == sessionLeasesId.get(store).get) {
      sessionLeases.remove(store)
      val awaitLeaseRenewal = context.system.scheduler.scheduleOnce(leaseTime.duration / 4, self, RenewTimeout(store))
      renewLeaseTimers.put(store, awaitLeaseRenewal)
      store ! RenewLease(transactionId)
    }
  }

  def handleLeaseRenewed(leaseTime: Timeout): Unit = {
    if (!(hasAborted || hasSentPrepares)) {
      val renewTimer = renewLeaseTimers.get(sender)
      if (renewTimer.isDefined) {
        renewTimer.get.cancel()
        renewLeaseTimers.remove(sender)
      } else {
        log.error("this timer should have been defined")
      }
      val renewScheduler = context.system.scheduler.scheduleOnce(leaseTime.duration / 2, self, LeaseTimeout(sender, leaseTime, sessionLeasesId.get(sender).get))
      sessionLeases.put(sender, renewScheduler)
    }
  }

  def handleRenewTimeout(store: ActorRef): Unit = {
    if (renewLeaseTimers.contains(store)) {
      abortEarly()
    }
  }

  def abortEarly(): Unit = {
    // cancel all timers
    if (!hasAborted) {
      hasAborted = true
      if (pendingAcquireWith.isDefined) {
        pendingAcquireWith.get ! RemoveFromWaitQueue(transactionId, pendingAcquireFor.get)
        pendingAcquireWith = None
        pendingAcquireFor = None
      }

      for (s <- sessionLeases) {
        // make sure that we correctly cancel all of these by incrementing sessionLease ids
        s._2.cancel()
        sessionLeasesId.put(sender, sessionLeasesId.get(s._1).get + 1)
        s._1 ! AbortEarly(transactionId)
      }
      for (r <- renewLeaseTimers) {
        r._2.cancel()
        renewLeaseTimers.remove(r._1)
      }
      if (acquireScheduler.isDefined) {
        acquireScheduler.get.cancel()
        acquireScheduler = None
        acquireSender.get ! TransactionAbortedEarly
      }
      if (votingScheduler.isDefined) {
        votingScheduler.get.cancel()
        votingScheduler = None
        commitSender.get ! Aborted
      }
    }
  }

  def handleCommit(cache: AnyMap): Unit = {
    if (hasAborted) {
      sender ! TransactionAbortedEarly
    } else {
      this.cache = Some(cache)
      for ((store, keys) <- storeToKeys) {
        store ! Prepare(transactionId, keys)
        votesPendingFrom.add(store)
      }

      hasSentPrepares = true
      // cancel lease timers
      for ((store, leaseTimer) <- sessionLeases) {
        // make sure we cancel these correctly by incrementing session lease IDs
        leaseTimer.cancel()
        sessionLeasesId.put(sender, sessionLeasesId.get(store).get + 1)
      }

      for ((store, renewTimer) <- renewLeaseTimers) {
        // these need to be cancelled correctly
        renewTimer.cancel()
        renewLeaseTimers.remove(store)
      }

      // schedule voting timeout
      votingScheduler = Some(context.system.scheduler.scheduleOnce(acquireWaitTime.duration, self, VotingTimeout))
      commitSender = Some(sender)
    }
  }

  def handleVoteCommit(): Unit = {
    votesPendingFrom.remove(sender)
    if (votesPendingFrom.isEmpty) {
      countVotes()
    }
  }

  def handleVoteAbort(): Unit = {
    votesPendingFrom.remove(sender)
    abortReceived = true
    if (votesPendingFrom.isEmpty) {
      countVotes()
    }
  }

  def countVotes(): Unit = {
    // make sure we cancel this correctly by setting it to None
    votingScheduler.get.cancel()
    votingScheduler = None
    if (abortReceived) {
      // send out aborts
      for ((store, leases) <- sessionLeases) {
        store ! AbortTransaction(transactionId)
        acksPendingFrom.add(store)
      }
      resendAbortScheduler = Some(context.system.scheduler.scheduleOnce(resendCommitTime.duration, self, ResendAbortTimeout))
      commitSender.get ! Aborted
    } else {
      for ((store, leases) <- sessionLeases) {
        store ! CommitTransaction(transactionId, constructKVPairs(store))
        acksPendingFrom.add(store)
      }
      resendCommitScheduler = Some(context.system.scheduler.scheduleOnce(resendCommitTime.duration, self, ResendCommitTimeout))
      commitSender.get ! Committed
    }

  }

  // for a given store, returns all the kv pairs that route to this store
  def constructKVPairs(store: ActorRef): scala.collection.Map[BigInt, Any] = {
    val keySet = storeToKeys.get(store)
    val subCache = new mutable.HashMap[BigInt, Any]()
    for ((k, v) <- cache.get){
      if(keySet.get.contains(k)){
        subCache.put(k,v)
      }
    }
    subCache
  }


  def handleVotingTimeout(): Unit = {
    // send abort to every store in session leases
    if (votingScheduler.isDefined) {
      votingScheduler = None
      for ((store, leases) <- sessionLeases) {
        store ! AbortTransaction(transactionId)
        acksPendingFrom.add(store)
      }
      commitSender.get ! Aborted

      // set timer to send aborts again
      resendAbortScheduler = Some(context.system.scheduler.scheduleOnce(resendCommitTime.duration, self, ResendAbortTimeout))
    }
  }

  def handleResendCommitTimeout(): Unit = {
    if (resendCommitScheduler.isDefined) {
      for (store <- acksPendingFrom) {
        store ! CommitTransaction(transactionId, constructKVPairs(store))
      }

      // set timer to send commits again
      resendCommitScheduler = Some(context.system.scheduler.scheduleOnce(resendCommitTime.duration, self, ResendCommitTimeout))
    }
  }

  def handleResendAbortTimeout(): Unit = {
    if (resendAbortScheduler.isDefined) {
      for (store <- acksPendingFrom) {
        store ! AbortTransaction(transactionId)
      }

      // set timer to send aborts again
      resendAbortScheduler = Some(context.system.scheduler.scheduleOnce(resendCommitTime.duration, self, ResendAbortTimeout))
    }
  }

  def handleAcknowledgement(): Unit = {
    acksPendingFrom.remove(sender)
    if (acksPendingFrom.isEmpty) {
      // this will be defined if we were aborting transaction
      if (resendAbortScheduler.isDefined) {
        // it is possible these don't cancel, we can just have a check in the resend methods to make sure we are still expecting an ack, otherwise do nothing
        resendAbortScheduler.get.cancel()
        resendAbortScheduler = None
        // send error message to aborted
        context.stop(self)
      }
      // this will be defined if we were committing transaction
      if (resendCommitScheduler.isDefined) {
        resendCommitScheduler.get.cancel()
        resendCommitScheduler = None
        // send error message to aborted
        context.stop(self)
      }
    }
  }



  import java.security.MessageDigest

  /** Generates a convenient hash key for an object to be written to the store.  Each object is created
    * by a given client, which gives it a sequence number that is distinct from all other objects created
    * by that client.
    */
  def hashForKey(nodeID: Int, cellSeq: Int): BigInt = {
    val label = "Node" ++ nodeID.toString ++ "+Cell" ++ cellSeq.toString
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(label.getBytes)
    BigInt(1, digest)
  }

  /**
    * @param key A key
    * @return An ActorRef for a store server that stores the key's value.
    */
  private def route(key: BigInt): ActorRef = {
    stores((key % stores.length).toInt)
  }
}

object KVClientHelper {
  def props(transactionId: String, stores: Seq[ActorRef], acquireWaitTime: Timeout): Props = {
    Props(classOf[KVClientHelper], transactionId, stores, acquireWaitTime)
  }
}
