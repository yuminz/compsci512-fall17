package rings

import akka.actor.{ActorRef, ActorSystem, DeadLetter, Props}
import akka.util.Timeout

import scala.collection.immutable.HashSet

sealed trait AppServiceAPI
case class Prime() extends AppServiceAPI
case class Command() extends AppServiceAPI
case class View(endpoints: Seq[ActorRef]) extends AppServiceAPI

/**
 * This object instantiates the service tiers and a load-generating master, and
 * links all the actors together by passing around ActorRef references.
 *
 * The service to instantiate is bolted into the KVAppService code.  Modify this
 * object if you want to instantiate a different service.
 */

object KVAppService {

  def apply(system: ActorSystem, numNodes: Int, ackEach: Int, leaseTime: Timeout, numLocks: Int, lockWaitTime: Timeout, partitionFrequency: Timeout): ActorRef = {

    val lockService = system.actorOf(LockService.props(leaseTime), "LockService")

    /** Service tier: create app servers */
    val servers = for (i <- 0 until numNodes)
      yield system.actorOf(LockingApplication.props(i, lockService, numLocks, new UniformPicker(), ackEach, lockWaitTime), "ApplicationServer" + i)

    // This was for debugging purposes
    //val deadLetterListener = system.actorOf(DeadLetterListener.props(), "DeadLetterListener")
    //system.eventStream.subscribe(deadLetterListener, classOf[DeadLetter])

    val clientIDSet = for (i <- 0 until numNodes) yield i
    val partitionSimulator = system.actorOf(PartitionSimulator.props(clientIDSet, lockService, partitionFrequency, leaseTime))
    /** Load-generating master */
    val master = system.actorOf(LoadMaster.props(numNodes, servers, ackEach, lockService), "LoadMaster")
    master
  }
}

