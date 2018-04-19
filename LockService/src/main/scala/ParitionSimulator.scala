package rings
import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.duration._

trait PartitionSimulatorApi
case object CreatePartition extends PartitionSimulatorApi
case class EndPartition(clients: Set[Int]) extends  PartitionSimulatorApi

class PartitionSimulator(clients: IndexedSeq[Int], lockService: ActorRef, partitionFrequency: Timeout, leaseTime: Timeout) extends Actor {
  import context.dispatcher

  val generator = new scala.util.Random

  private val partitionTime = Timeout(3 seconds)

  private val clientsNotYetPartitioned = new mutable.HashSet[Int]()
  private val clientsPartitioned = new mutable.HashSet[Int]()

  for (c <- clients) {
    clientsNotYetPartitioned.add(c)
  }

  private var partitionScheduler = context.system.scheduler.scheduleOnce(partitionFrequency.duration, self, CreatePartition)


  override def receive: Receive = {
    case CreatePartition => createPartition()
    case EndPartition(clients: Set[Int]) => endPartition(clients)
  }

  private def createPartition(): Unit = {
    if (isPartition()) {
      if (clientsNotYetPartitioned.nonEmpty) {
        // randomly pick size and members of partition
        val sizeOfPartition = chooseSizeOfPartition()
        val pClients = new mutable.HashSet[Int]()
        var i = 0
        while (i < sizeOfPartition) {
          val clientsVector = clientsNotYetPartitioned.toVector.sorted
          val c = clientsVector(generator.nextInt(clientsVector.size))
          pClients.add(c)
          clientsPartitioned.add(c)
          clientsNotYetPartitioned.remove(c)
          i += 1
        }
        val newPartition = pClients.toSet
        val partitionDuration = leaseTime.duration
        println(s"Network partition occurring for $partitionDuration and client(s) with ids: $newPartition affected")
        lockService ! BeginPartitionWith(newPartition)
        context.system.scheduler.scheduleOnce(partitionDuration, self, EndPartition(newPartition))
      }
    }
    partitionScheduler = context.system.scheduler.scheduleOnce(partitionFrequency.duration, self, CreatePartition)
  }

  private def endPartition(refs: Set[Int]): Unit = {
    for (c <- refs) {
      clientsNotYetPartitioned.add(c)
      clientsPartitioned.remove(c)
    }
    lockService ! EndPartitionWith(refs)
  }

  // We currently always make isPartition return true so that every time the createPartition timer goes off, a network partition occurs
  // If we were modeling more of a real world scenario, we might change 100 to be something less so that network partitions
  // don't happen at a regular rate
  private def isPartition(): Boolean = {
    generator.nextInt(100) < 100
  }

  private def chooseSizeOfPartition(): Int = {
    if (clientsNotYetPartitioned.size > 1) {
      generator.nextInt(clientsNotYetPartitioned.size - 1) + 1
    } else {
      1
    }
  }




}

object PartitionSimulator {
  def props(clients: IndexedSeq[Int], lockService: ActorRef, partitionFrequency: Timeout, leaseTime: Timeout): Props = {
    Props(classOf[PartitionSimulator], clients, lockService, partitionFrequency, leaseTime)
  }
}
