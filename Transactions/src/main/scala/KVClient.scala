package rings

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable

class AnyMap extends scala.collection.mutable.HashMap[BigInt, Any]

/**
 * KVClient implements a client's interface to a KVStore.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers
  * @param myNodeID the integer identifier of the actor instantiating the client
  * @param actorSystem the actor system passed in by the actor instantiating the client - used to create KVClientHelper actors
  * @param acquireWaitTime how long the actor should block when trying to acquire a lock on a key
 */

trait AcquireApi
case object AcquireFailed extends AcquireApi
case class AcquireSuccess(value: Option[Any]) extends AcquireApi

trait CommitApi
case object Aborted extends CommitApi
case object Committed extends CommitApi

case object TransactionAbortedEarly extends AcquireApi with CommitApi

class KVClient (stores: Seq[ActorRef], myNodeID: Int, actorSystem: ActorSystem, acquireWaitTime: Timeout) {
  val log = Logging.getLogger(actorSystem, this)
  private val cache = new AnyMap
  private val locksHeld = new mutable.HashSet[BigInt]()

  private var helper: Option[ActorRef] = None
  var transactionCounter = 0
  var currentTransactionId: Option[String] = None
  import scala.concurrent.ExecutionContext.Implicits.global

  def begin(): Unit = {
    if (helper.isDefined) {
      throw new TransactionAlreadyBegunException
    } else {
      transactionCounter += 1
      currentTransactionId = Some(myNodeID + "_" + transactionCounter)
      helper = Some(actorSystem.actorOf(KVClientHelper.props(currentTransactionId.get, stores, acquireWaitTime), "ClientHelper" + currentTransactionId.get))
    }
  }

  def read(key: BigInt): Option[Any] = {
    if (helper.isDefined) {
      // if the value is in the cache, that means we have permissions for this item
      if (locksHeld.contains(key)) {
        cache.get(key)
        // if value is empty, that means that we don't have permissions and must tell helper to acquire lock
      } else {
        val future = ask(helper.get, Read(key))(Timeout(acquireWaitTime.duration * 2)).mapTo[AcquireApi]
        val result = Await.result(future, acquireWaitTime.duration * 2)
        // check if acquire success or failed
        result match {
          case AcquireSuccess(v) => {
            locksHeld.add(key)
            if (v.isDefined) {
              cache.put(key, v.get)
            }
            v
          }
          case AcquireFailed => {
            // we couldn't acquire the lock in time
            // abort stuff in client
            cleanupTransaction()
            throw new AcquireTimeoutException
          }
          case TransactionAbortedEarly => {
            // another lease we currently hold couldn't be renewed so it expired
            cleanupTransaction()
            throw new TransactionAbortedEarlyException
          }
        }
      }
    } else {
      throw new NoExistingTransactionException
    }
  }

  def write(key: BigInt, value: Any): Unit = {
    if (helper.isDefined) {
      // if the value is in the cache, that means we have permissions for this item
      if (locksHeld.contains(key)) {
        cache.put(key, value)
      } else {
        // if value is empty, that means that we don't have permissions and must tell helper to acquire lock
        val future = ask(helper.get, Write(key))(Timeout(acquireWaitTime.duration * 2)).mapTo[AcquireApi]
        val result = Await.result(future, acquireWaitTime.duration * 2)
        // check if acquire success or failed
        result match {
          case AcquireSuccess(v) => {
            locksHeld.add(key)
            cache.put(key, value)
          }
          case AcquireFailed => {
            // abort stuff in client
            cleanupTransaction()
            throw new AcquireTimeoutException
          }
          case TransactionAbortedEarly => {
            cleanupTransaction()
            throw new TransactionAbortedEarlyException
          }
        }

      }
    } else {
      throw new NoExistingTransactionException
    }
  }

  def commit(): Unit = {
    if (helper.isDefined) {
      val future = ask(helper.get, Commit(cache))(Timeout(acquireWaitTime.duration * 2)).mapTo[CommitApi]
      val response = Await.result(future, acquireWaitTime.duration * 2)
      response match {
        case Committed => {
          // do nothing for now except cleanup state in client
          cleanupTransaction()
        }
        case Aborted => {
          // a unanimous vote to commit couldn't be obtained
          cleanupTransaction()
          throw new TransactionAbortedException
        }
        case TransactionAbortedEarly => {
          cleanupTransaction()
          throw new TransactionAbortedEarlyException
        }
      }
    } else {
      throw new NoExistingTransactionException
    }
  }

  def abort(): Unit = {
    if (helper.isDefined) {
      helper.get ! Abort
      cleanupTransaction()
    } else {
      throw new NoExistingTransactionException
    }
  }


  private def cleanupTransaction(): Unit = {
    helper = None
    currentTransactionId = None
    cache.clear()
    locksHeld.clear()
  }
}

// Thrown when the user already has an in progress transaction and calls begin() again
class TransactionAlreadyBegunException extends Exception {

}

// Thrown when the user calls read/write/commit/abort without first starting a transaction with begin()
class NoExistingTransactionException extends Exception {

}

// Thrown when a unanimous vote to commit can't be obtained
class TransactionAbortedException extends Exception {

}

// Thrown when the lock for a given key can't be obtained within acquireTimeout
class AcquireTimeoutException extends Exception {

}

// Thrown when the the lease for one of our locks couldn't be renewed
class TransactionAbortedEarlyException extends Exception {

}