package rings
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.duration._

trait PartitionSimulatorApi
case object CreatePartition extends PartitionSimulatorApi

/**
  * This partition simulator will periodically tell the KVStores which clients are currently partitioned from them.
  * In this implementation of PartitionSimulator, if a client is partitioned from one KVStore, they are partitioned from every KVStore.
  * @param clients The set of clientIDs who can possible be partitioned from the stores
  * @param stores
  * @param partitionFrequency How often we update the set of clients who are partitioned from the stores
  */
class PartitionSimulator(clients: IndexedSeq[Int], stores: IndexedSeq[ActorRef], partitionFrequency: Timeout) extends Actor {
  import context.dispatcher

  val log = Logging.getLogger(context.system, this)

  val generator = new scala.util.Random

  // Every time this timer goes off, we update the set of clients who are currently partitioned from the KVStores
  private var partitionScheduler = context.system.scheduler.scheduleOnce(partitionFrequency.duration, self, CreatePartition)


  override def receive: Receive = {
    case CreatePartition => createPartition()
  }

  private def createPartition(): Unit = {
    for (store <- stores) {
      val partitionedSet = new mutable.HashSet[Int]()
      for (clientId <- clients) {
        if (generator.nextInt(100) < 30) {
          partitionedSet.add(clientId)
        }
      }
      log.info(s"$store is now partitioned from $partitionedSet")
      store ! UpdatePartitions(partitionedSet.toSet)
    }
    partitionScheduler = context.system.scheduler.scheduleOnce(partitionFrequency.duration, self, CreatePartition)
  }
}

object PartitionSimulator {
  def props(clients: IndexedSeq[Int], stores: IndexedSeq[ActorRef], partitionFrequency: Timeout): Props = {
    Props(classOf[PartitionSimulator], clients, stores, partitionFrequency)
  }
}
