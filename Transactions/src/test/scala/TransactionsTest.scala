package rings

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.FlatSpecLike

import scala.collection.mutable.{HashSet, Set}
import scala.concurrent.Await
import scala.concurrent.duration._


// this is used for our final test '2PC protocol must force participants to block on awaiting a commit/abort message if they have already voted'
class ManInTheMiddle(store: ActorRef, clientId: Int) extends Actor {
  var client: ActorRef = null
  override def receive: Receive = {
    case VoteCommit => {
      store ! UpdatePartitions(scala.collection.immutable.Set(clientId))
      client ! VoteCommit
    }
    case x: Any if x.isInstanceOf[KVStoreAPI] => {
      client = sender
      store ! x
    }
    case x: Any if x.isInstanceOf[KVClientHelperApi] => {
      client ! x
    }
  }
}

object ManInTheMiddle {
  def props(store: ActorRef, clientId: Int): Props = {
    Props(classOf[ManInTheMiddle], store, clientId)
  }
}

trait MockApplicationApi
case class ReadValue(key: BigInt) extends MockApplicationApi
case object IsDeadlocked extends MockApplicationApi

class MockApplication(stores: Seq[ActorRef], clientId: Int) extends Actor {
  val kvClient = new KVClient(stores, clientId, context.system, Timeout(1 seconds))
  kvClient.begin()

  override def receive = {
    case ReadValue(key: BigInt) => {
      try {
        kvClient.read(key)
      } catch {
        case ex: AcquireTimeoutException => {}
      }
    }
    case IsDeadlocked => {
      sender ! false
    }
  }
}

object MockApplication {
  def props(stores: Seq[ActorRef], clientId: Int): Props = {
    Props(classOf[MockApplication], stores, clientId)
  }
}

class TransactionsTest extends TestKit(ActorSystem("TransactionsTest")) with ImplicitSender with FlatSpecLike {
  def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  // used for 'Transactions must read what they write'
  def runSample(kvs: Seq[ActorRef], id: Int): Unit = {
    val client = new KVClient(kvs, id, system, Timeout(10 seconds))

    client.begin()
    client.write(0, id)
    val readV = client.read(0)
    client.commit()
    assert(readV.isDefined)
    assert(readV.get == id)
  }

  "Transactions" must "read what they write" in {
    // this tests isolation of concurrent transactions writing and reading the same key
    val kv = system.actorOf(KVStore.props(Timeout(1 second)))
    val kvs = Seq(kv)

    val threads : Set[Thread] = new HashSet[Thread]
    for (i <- 0 until 5) {
      val newT = new Thread(new Runnable {
        override def run {
          runSample(kvs, i)
        }
      })

      threads += newT
      newT.start
    }

    for (t <- threads)
      t.join
  }

  // used for the next two tests to give the two keys some initial values
  def prepare(c0: KVClient, c1: KVClient): Unit = {
    c0.begin()
    c0.write(1, "Orig-V-1")
    c0.write(2, "Orig-V-2")
    assert(c0.read(2).get == "Orig-V-2")
    c0.commit()
  }

  "A transaction" must "read modifications from previously committed transactions" in {
    // this test helps verify atomicity and isolation of transactions
    val leaseTime = Timeout(1 second)
    val acquireWaitTime = Timeout(5 seconds)
    val kv = system.actorOf(KVStore.props(leaseTime))
    val kvs = Seq(kv)

    val c0 = new KVClient(kvs, 100, system, acquireWaitTime)
    val c1 = new KVClient(kvs, 101, system, acquireWaitTime)

    prepare(c0, c1)

    c0.begin()
    c0.write(1, "New-V-1")
    c0.write(2, "New-V-2")
    c0.commit()

    c1.begin()
    assert(c1.read(1).get == "New-V-1")
    assert(c1.read(2).get == "New-V-2")
    c1.commit()
  }

  "A transaction" must "not read updates from aborted transactions" in {
    // this test helps verify atomicity and isolation of transactions
    val leaseTime = Timeout(1 second)
    val acquireWaitTime = Timeout(5 seconds)
    val kv = system.actorOf(KVStore.props(leaseTime))
    val kvs = Seq(kv)

    val c0 = new KVClient(kvs, 200, system, acquireWaitTime)
    val c1 = new KVClient(kvs, 201, system, acquireWaitTime)

    prepare(c0, c1)

    c0.begin()
    c0.write(1, "New-V-1")
    c0.write(2, "New-V-2")
    c0.abort()

    c1.begin()
    assert(c1.read(1).get == "Orig-V-1")
    assert(c1.read(2).get == "Orig-V-2")
    c1.commit()
  }

  /**
    * We create two mock applications, app0 acquiring lock0 and app1 acquiring lock1. We then tell app0 to acquire
    * lock1 and app1 to acquire lock0, thus creating a circular waiting for resources -> potentially causing deadlock.
    * However, we show that after acquireTimeout, at least one of the transactions will abort, so we no longer
    * have deadlock
    */
  "Transactions" must "not stay deadlocked forever" in {
    val leaseTime = Timeout(1 second)
    val kv = system.actorOf(KVStore.props(leaseTime))
    val kvs = Seq(kv)

    val app0 = system.actorOf(MockApplication.props(kvs, 202))
    val app1 = system.actorOf(MockApplication.props(kvs, 203))

    app0 ! ReadValue(0)
    app1 ! ReadValue(1)

    Thread.sleep(500)

    app0 ! ReadValue(1)
    app1 ! ReadValue(0)

    val future0 = ask(app0, IsDeadlocked)(3 second).mapTo[Boolean]
    assert(!Await.result(future0, 3 second))

    val future1 = ask(app1, IsDeadlocked)(3 second).mapTo[Boolean]
    assert(!Await.result(future1, 3 second))
  }

  /**
    * We test that if a transaction acquire locks from a store but then is partitioned from that store when it sends
    * out prepare messages, the transaction will abort. This is essentially testing that we handle the VotingTimeout
    * correctly in KVClientHelper. It also tests that when we count votes, we get a unanimous Commit before deciding to commit.
    * Because the client is only partitioned from one of the stores, it received a VoteCommit from the other store.
    * Note that we set the longLeaseTime so high because we want the transaction to
    * abort due to the VotingTimeout (acquireWaitTime) and not because first one of the leases expired due to the partition.
    */
  "2PC protocol" must "cause a transaction to abort if a participant is partitioned from the coordinator" in {
    // we set a long
    val longLeaseTime = Timeout(15 second)
    val acquireWaitTime = Timeout(2 seconds)
    val kv0 = system.actorOf(KVStore.props(longLeaseTime))
    val kv1 = system.actorOf(KVStore.props(longLeaseTime))
    val kvs = Seq(kv0, kv1)

    val c0 = new KVClient(kvs, 300, system, acquireWaitTime)
    c0.begin()
    c0.write(1, "hello world")
    c0.write(2, "hello world")
    // before commit start partition with one store
    kv0 ! UpdatePartitions(scala.collection.immutable.Set[Int](300))
    Thread.sleep(1000)
    var transactionAborted = false
    try {
      c0.commit()
    } catch {
      case ex: TransactionAbortedException => {
        transactionAborted = true
      }
    }
    // see that transaction aborts because couldn't get the vote
    assert(transactionAborted)
  }

  /**
    * This test verifies that once a Store receives a prepare message and it votes to commit, it must block until
    * it receives a CommitTransaction/AbortTransaction message from the client. This means that no other transaction
    * can acquire the same locks before this one commits, and we demonstrate that while the store is blocking, some other
    * client throws a AcquireTimeoutException when trying to acquire the lock. The difficult part of this test is making
    * the network partition between the coordinator and participant happen after the prepare messages are received but before
    * the coordinator sends a CommitTransaction message to the participant. To make this possible, we introduce a proxy
    * in between the two that simply forwards messages from one to the other. The special case is when it receives a
    * VoteCommit message from the participant, at which point it forwards the message to the client and starts a network
    * partition on the store.
    */
  "2PC protocol" must "force participants to block on awaiting a commit/abort message if they have already voted" in {
    val clientId = 400
    val leaseTime = Timeout(1 second)
    val acquireWaitTime = Timeout(2 seconds)
    val kv = system.actorOf(KVStore.props(leaseTime), "store")
    val wrapper = system.actorOf(ManInTheMiddle.props(kv, clientId), "maninmiddle")
    val kvs = Seq(wrapper)
    val c0 = new KVClient(kvs, clientId, system, acquireWaitTime)
    val c1 = new KVClient(Seq(kv), clientId + 1, system, acquireWaitTime)
    c0.begin()
    c0.read(1)
    c0.commit()

    c1.begin()
    var unableToAcquireLock = false
    try {
      c1.read(1)
    } catch {
      case ex: AcquireTimeoutException => {
        unableToAcquireLock = true
      }
    }
    assert(unableToAcquireLock)
  }
}
