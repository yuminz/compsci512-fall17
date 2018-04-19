package rings
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import akka.pattern.ask
import org.scalatest.FlatSpecLike

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

trait MockApplicationApi
case object HasLock
case class TryAcquire(lock: String)
case class ReleaseLock(lock: String)

class MockApplication(val myNodeID: Int, lockService: ActorRef, lockWaitTime: Timeout) extends Actor {

  val lockClient = new LockClient(myNodeID, lockService, this, lockWaitTime)
  var hasLock = false

  override def receive: Receive = {
    case TryAcquire(lock) => {
      hasLock = lockClient.acquire(lock)
    }
    case HasLock => {
      sender ! hasLock
    }
    case ReleaseLock(lock) => {
      lockClient.release(lock)
      hasLock = false
    }
    case LockLost(lock) => {
      hasLock = false
    }
  }
}

object MockApplication {
  def props(myNodeID: Int, lockService: ActorRef, lockWaitTime: Timeout): Props = {
    Props(classOf[MockApplication], myNodeID, lockService, lockWaitTime)
  }
}

class LockServiceTest extends TestKit(ActorSystem("LockServiceTest")) with ImplicitSender with FlatSpecLike {
  println("testing in progress...")

  val testLock = "testLock"

  /*Tests:
  Mutual exclusion
    have a few test actors all try to acquire the same lock. We then loop over the actors and see that only one holds
    the lock. We can then release and make sure that only one still has the lock

  Make sure network partition actually ignores things, but only those things:
  Have a lock holder never receive response for lease renew

  Lock holder gets partitioned, Someone else come in, requests lock, is granted the lock.
  Make sure app that lost the lock is notified

  Without network partition, when lock holder not actively holding lock and someone else requests it, lock is granted to other right away
*/
  def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Lock Service" must "allow at most one actor to hold a given lock" in {
    val timeout = Timeout(1 second)
    val lockService = system.actorOf(LockService.props(timeout), "Test1-LockServer1")
    val clients = new mutable.HashSet[ActorRef]()
    for (i <- 0 until 5) {
      val c = system.actorOf(MockApplication.props(i, lockService, timeout), "Test1-ClientActor" + i)
      c ! TryAcquire(testLock)
      clients.add(c)
    }
    Thread.sleep(2000)
    var numLockHolders = 0
    var currentLockHolder: ActorRef = null
    for (c <- clients) {
      val future = ask(c, HasLock)(timeout).mapTo[Boolean]
      if (Await.result(future, timeout.duration)) {
        currentLockHolder = c
        numLockHolders += 1
      }
    }
    assert(numLockHolders <= 1)

    if (currentLockHolder != null) {
      currentLockHolder ! ReleaseLock(testLock)
    }

    for (c <- clients) {
      if (c != currentLockHolder) {
        c ! TryAcquire(testLock)
      }
    }
    Thread.sleep(2000)
    numLockHolders = 0
    for (c <- clients) {
      val future = ask(c, HasLock)(timeout).mapTo[Boolean]
      if (Await.result(future, timeout.duration)) {
        currentLockHolder = c
        numLockHolders += 1
      }
    }
    assert(numLockHolders <= 1)
  }

  "Lock Service " must "correctly ignore messages from a partition client" in {
    // In this test, actor1 obtains a lock successfully, but is then partitioned from the LockServer. We show that
    // after the lease runs out, the actor wasn't able to renew the lease so it no longer holds the lock.

    val timeout = Timeout(1 second)
    val lockService = system.actorOf(LockService.props(timeout), "Test2-LockServer")
    val actor1 = system.actorOf(MockApplication.props(1, lockService, timeout), "Test2-TestActor1")

    actor1 ! TryAcquire(testLock)
    Thread.sleep(500)
    var future = ask(actor1, HasLock)(timeout).mapTo[Boolean]
    assert(Await.result(future, timeout.duration))
    lockService ! BeginPartitionWith(Set(1))
    Thread.sleep(timeout.duration.toMillis * 2)
    future = ask(actor1, HasLock)(timeout).mapTo[Boolean]
    assert(!Await.result(future, timeout.duration))
  }

  "Lock Service" must "correctly seizes the lock from the partitioned holder and grants it to another client waiting" in {
    val timeout = Timeout(2 second)
    val lockAcquireWaitTime = Timeout(4 seconds)
    val lockService = system.actorOf(LockService.props(timeout), "Test3-LockServer")
    val actor1 = system.actorOf(MockApplication.props(1, lockService, lockAcquireWaitTime), "Test3-TestActor1")
    val actor2 = system.actorOf(MockApplication.props(2, lockService, lockAcquireWaitTime), "Test3-TestActor2")

    actor1 ! TryAcquire("testLock")
    Thread.sleep(500)
    var future = ask(actor1, HasLock)(timeout).mapTo[Boolean]
    assert(Await.result(future, timeout.duration))
    actor2 ! TryAcquire(testLock)
    // assert 2nd actor doesn't hold the lock before the partition
    future = ask(actor2, HasLock)(Timeout(5 seconds)).mapTo[Boolean]
    assert(!Await.result(future, 5 seconds))
    actor2 ! TryAcquire(testLock)
    lockService ! BeginPartitionWith(Set(1))
    Thread.sleep(timeout.duration.toMillis * 3)
    // assert 2nd actor DOES hold the lock after the partition
    future = ask(actor2, HasLock)(timeout).mapTo[Boolean]
    assert(Await.result(future, timeout.duration))
  }

  "Lock client" must "cache lock if released and no one else actually requests lock" in {
    val timeout = Timeout(2 second)
    val lockAcquireWaitTime = Timeout(4 seconds)
    val lockService = system.actorOf(LockService.props(timeout), "Test4-LockServer")
    val holderId = 1
    val actor1 = system.actorOf(MockApplication.props(holderId, lockService, lockAcquireWaitTime), "Test4-TestActor1")

    actor1 ! TryAcquire(testLock)
    Thread.sleep(500)
    var future = ask(actor1, HasLock)(timeout).mapTo[Boolean]
    assert(Await.result(future, timeout.duration))
    actor1 ! ReleaseLock(testLock)
    Thread.sleep(500)
    future = ask(actor1, HasLock)(timeout).mapTo[Boolean]
    assert(!Await.result(future, timeout.duration))
    // We wait so that another renew message is sent from the client saying it is no longer actively using the lock
    Thread.sleep(1000)

    // check that lock is still cached by client by querying the lock manager
    val futureManager = ask(lockService, FindLockManager(0, testLock))(timeout).mapTo[ActorRef]
    val lockManager = Await.result(futureManager, timeout.duration)

    val futureLockInfo = ask(lockManager, GetLockInfo)(timeout).mapTo[Option[Int]]
    val lockIsActivelyUsedByHolder = Await.result(futureLockInfo, timeout.duration)
    assert(lockIsActivelyUsedByHolder.get == holderId)
  }
}

