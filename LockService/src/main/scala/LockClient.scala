package rings
import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._


class LockClient(val myNodeID: Int, val lockService: ActorRef, val appActor: Actor, val lockWaitTime: Timeout) {

  private val helperMap: mutable.Map[String, ActorRef] = new mutable.HashMap[String, ActorRef]

  def acquire(lock: String): Boolean = {
    // This is if helper isn't yet created
    try {
      if (!helperMap.contains(lock)) {
        // send findLockManager to lockService
        val future = ask(lockService, FindLockManager(myNodeID, lock))(lockWaitTime).mapTo[ActorRef]
        val lockManager = Await.result(future, lockWaitTime.duration)
        val helper = appActor.context.actorOf(LockClientHelper.props(myNodeID, lockManager, appActor.self, lock), "LockHelper" + lock)
        helperMap.put(lock, helper)
      }
      val helper = helperMap.get(lock).get
      // Tell helper we want to acquire lock
      val future = ask(helper, AcquireLock(lockWaitTime))(lockWaitTime.duration * 3).mapTo[Boolean]
      Await.result(future, Duration.Inf)
    } catch {
      case e: TimeoutException => {
        false
      }
    }
  }

  def release(lock: String): Unit = {
    // send message to appropriate helper
    if (!helperMap.contains(lock)) {
      throw new RuntimeException("lock not held")
    } else {
      helperMap.get(lock).get ! Release
    }
  }

  def getStats: LockStats = {
    val aggregateLockStats = new LockStats
    for (h <- helperMap.values) {
      val future = ask(h, GetFinalStats)(1 second).mapTo[LockStats]
      val current = Await.result(future, 1 second)
      aggregateLockStats += current
    }
    aggregateLockStats
  }
}
