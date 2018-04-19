package slicer

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (service: ActorRef, parent: Actor) {
  // create helper actor

  private val helper = parent.context.actorOf(ClientHelper.props(service), "Helper")

  // forward all messages to client helper
  def execute(key: Long, latency: Int): Unit = {
    helper ! Execute(key, latency)
  }

  def close(): Unit = {
    helper ! Close
  }
}

sealed trait ClientHelperAPI
case class Execute(key: Long, latency: Int) extends ClientHelperAPI
case class UpdateMapping(mapping: Vector[PartitionInfo]) extends ClientHelperAPI
case object Close extends ClientHelperAPI

class ClientHelper (service: ActorRef) extends Actor {
  val log = Logging.getLogger(context.system, self)
  val randomGenerator = new Random()

  // register with service to get initial mapping
  private val future = ask(service, RegisterClient(self))(Timeout(1 second)).mapTo[UpdateMapping]
  private var partitionMapping = Await.result(future, 1 second).mapping
  //log.info("initial mapping is " + partitionMapping.toString())

  // TODO on shutdown deregister with service

  override def receive: Receive = {
    case Execute(key, latency) => execute(key, latency)
    case UpdateMapping(mapping) => updateMapping(mapping)
    case Close => close()
  }

  def execute(key: Long, latency: Int): Unit = {
    try {
      route(key) ! Operation(key, latency)
    } catch {
      case e: NullPointerException => {}
    }
  }

  def close(): Unit = {
    service ! DeregisterClient
  }

  def updateMapping(mapping: Vector[PartitionInfo]): Unit = {
    //log.info("new mapping is " + mapping.toString())
    partitionMapping = mapping
  }

  private def getRandomServer(servers: List[ActorRef]): ActorRef = {
    val idx = randomGenerator.nextInt(servers.length)
    servers(idx)
  }

  /**
    * @param key A key
    * @return An ActorRef for a store server that stores the key's value.
    */
  private def route(key: Long): ActorRef = {
    val info = binarySearch(partitionMapping, key)
    if (info.isDefined) {
      // TODO pick one of the servers randomly
      getRandomServer(info.get.servers)
    } else {
      // TODO somehow this key isn't in the mapping?
      //log.info(s"no mapping for key $key")
      null
    }
  }

  private def binarySearch(l: Vector[PartitionInfo], k: Long): Option[PartitionInfo] = {
    val sorted = l.sortBy(_.start)
    var low = 0
    var hi = sorted.length - 1
    while (low <= hi) {
      val mid = (low + hi) / 2
      val midInfo = sorted(mid)
      if (k < midInfo.start) {
        hi = mid - 1
      } else if (k < midInfo.start + midInfo.size) {
        // lies in this partition
        return Some(midInfo)
      } else {
        low = mid + 1
      }
    }
    None
  }
}

object ClientHelper {
  def props(service: ActorRef): Props = {
    Props(classOf[ClientHelper], service)
  }
}
