package rings

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await

sealed trait LoadMasterAPI
case class Start(maxPerNode: Int) extends LoadMasterAPI
case class BurstAck(senderNodeID: Int, stats: Stats) extends LoadMasterAPI
case class Join() extends LoadMasterAPI

/** LoadMaster is a singleton actor that generates load for the app service tier, accepts acks from
  * the app tier for each command burst, and terminates the experiment when done.  It uses the incoming
  * acks to self-clock the flow of commands, to avoid swamping the mailbox queues in the app tier.
  * It also keeps running totals of various Stats returned by the app servers with each burst ack.
  * A listener may register via Join() to receive a message when the experiment is done.
  *
  * @param numNodes How many actors/servers in the app tier
  * @param servers ActorRefs for the actors/servers in the app tier
  * @param burstSize How many commands per burst
  */

class LoadMaster (val numNodes: Int, val servers: Seq[ActorRef], val burstSize: Int, lockService: ActorRef) extends Actor {
  val log = Logging(context.system, this)
  var active: Boolean = true
  var listener: Option[ActorRef] = None
  var nodesActive = numNodes
  var maxPerNode: Int = 0

  val serverStats = for (s <- servers) yield new Stats

  def receive = {
    case Start(totalPerNode) =>
      println("Master starting bursts")
      maxPerNode = totalPerNode
      for (s <- servers) {
        s ! Prime()
        burst(s)
      }

    case BurstAck(senderNodeID: Int, stats: Stats) =>
      serverStats(senderNodeID) += stats
      val s = serverStats(senderNodeID)
      if (s.messages == maxPerNode) {
        nodesActive -= 1
        if (nodesActive == 0)
          deactivate()
      } else {
        if (active)
          burst(servers(senderNodeID))
      }

    case Join() =>
      listener = Some(sender)
  }

  def burst(server: ActorRef): Unit = {
//    println(s"send a burst to node $target")
    for (i <- 1 to burstSize)
      server ! Command()
  }

  def getFinalStats(): Unit = {
    for (serverID <- serverStats.indices) {
      val future = ask(servers(serverID), SendStats)(Timeout(1 second)).mapTo[Stats]
      val finalStats = Await.result(future, 1 second)
      serverStats(serverID) += finalStats
    }
  }

  def deactivate() = {
    active = false
    // Before shutting down, we want to collect all lock specific stats from the application servers
    getFinalStats()
    val total = new Stats
    serverStats.foreach(total += _)
    println(s"$total")
    if (listener.isDefined)
      listener.get ! total
  }
}

object LoadMaster {
   def props(numNodes: Int, servers: Seq[ActorRef], burstSize: Int, lockService: ActorRef): Props = {
      Props(classOf[LoadMaster], numNodes, servers, burstSize, lockService)
   }
}

