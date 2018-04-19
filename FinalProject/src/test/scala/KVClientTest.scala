package slicer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{FlatSpec, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Await

object GiveKvClient
class KVClientCreator(service: ActorRef) extends Actor {
  val kvClient = new KVClient(service, this)

  override def receive: Receive = {
    case GiveKvClient => {
      sender ! kvClient
    }
  }
}

class MockSlicerService(mapping: Vector[PartitionInfo]) extends Actor {
  override def receive = {
    case RegisterClient(clientRef) => {
      sender ! UpdateMapping(mapping)
    }
  }
}

object MockSlicerService {
  def props(mapping: Vector[PartitionInfo]): Props = {
    Props(classOf[MockSlicerService], mapping)
  }
}


object GetKeysReceived
class MockKVCache() extends Actor {
  val opSet = new mutable.HashSet[Long]()

  override def receive: Receive = {
    case Operation(key, duration) => {
      opSet.add(key)
    }
    case GetKeysReceived => {
      sender ! opSet
    }
  }
}

object MockKVCache {
  def props(): Props = {
    Props(classOf[MockKVCache])
  }
}

class KVClientTest extends TestKit(ActorSystem("KVClientTest")) with ImplicitSender with FlatSpecLike {

  def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  println("test starting")

  "KVClient" must "route a given operation to the correct cache" in {
    // create cache actors
    val numCaches = 4
    val maxKey = 16
    val caches = mutable.MutableList[ActorRef]()
    for (i <- 0 until numCaches) {
      caches += system.actorOf(MockKVCache.props())
    }

    val mapping = Vector[PartitionInfo](new PartitionInfo(0, 4, List(caches.get(0).get)), new PartitionInfo(4, 4, List(caches.get(1).get)), new PartitionInfo(8, 4, List(caches.get(2).get)), new PartitionInfo(12, 4, List(caches.get(3).get)))

    val service = system.actorOf(MockSlicerService.props(mapping))

    // send client mapping
    val clientActor = system.actorOf(Props(classOf[KVClientCreator], service))
    val future = ask(clientActor, GiveKvClient)(Timeout(2 seconds)).mapTo[KVClient]
    val client = Await.result(future, 2 seconds)

    // send client series of requests to make sure forwards correctly
    client.execute(1, 10)
    client.execute(7, 10)
    client.execute(8, 10)
    client.execute(9, 10)
    client.execute(15, 10)

    Thread.sleep(1000)

    val future0 = ask(caches.get(0).get, GetKeysReceived)(Timeout(2 seconds)).mapTo[mutable.Set[Long]]
    val set0 = Await.result(future0, 2 seconds)
    println(set0)
    assert(set0.contains(1) && set0.size == 1)

    val future1 = ask(caches.get(1).get, GetKeysReceived)(Timeout(2 seconds)).mapTo[mutable.Set[Long]]
    val set1 = Await.result(future1, 2 seconds)
    assert(set1.contains(7) && set1.size == 1)

    val future2 = ask(caches.get(2).get, GetKeysReceived)(Timeout(2 seconds)).mapTo[mutable.Set[Long]]
    val set2 = Await.result(future2, 2 seconds)
    assert(set2.contains(8) && set2.contains(9) && set2.size == 2)

    val future3 = ask(caches.get(3).get, GetKeysReceived)(Timeout(2 seconds)).mapTo[mutable.Set[Long]]
    val set3 = Await.result(future3, 2 seconds)
    assert(set3.contains(15) && set3.size == 1)
  }
}