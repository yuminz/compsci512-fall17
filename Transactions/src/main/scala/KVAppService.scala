package rings

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout

sealed trait AppServiceAPI
case class Command() extends AppServiceAPI


/**
 * This object instantiates the service tiers and a load-generating master, and
 * links all the actors together by passing around ActorRef references.
 *
 * The service to instantiate is bolted into the KVAppService code.  Modify this
 * object if you want to instantiate a different service.
 */

object KVAppService {

  def apply(system: ActorSystem, numNodes: Int, ackEach: Int, partitionFrequency: Timeout, leaseTime: Timeout, acquireWaitTime: Timeout, numKeys: Int): ActorRef = {

    /** Storage tier: create K/V store servers */
    val stores = for (i <- 0 until numNodes)
      yield system.actorOf(KVStore.props(leaseTime), "Store" + i)

    /** Service tier: create app servers */
    val servers = for (i <- 0 until numNodes)
      yield system.actorOf(TransactionApplication.props(i, numNodes, stores, ackEach, acquireWaitTime: Timeout, numKeys), "TransactionApplication" + i)

    /** If you want to initialize a different service instead, that previous line might look like this:
      * yield system.actorOf(GroupServer.props(i, numNodes, stores, ackEach), "GroupServer" + i)
      * For that you need to implement the GroupServer object and the companion actor class.
      * Following the "rings" example.
      */

    val clientIds = for {
      id <- 0 until numNodes
    }  yield id

    val partitionSimulator = system.actorOf(PartitionSimulator.props(clientIds, stores, partitionFrequency))

    /** Load-generating master */
    val master = system.actorOf(LoadMaster.props(numNodes, servers, ackEach), "LoadMaster")
    master
  }
}

