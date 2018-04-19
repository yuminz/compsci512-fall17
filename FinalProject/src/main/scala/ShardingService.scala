package slicer

import scala.language.postfixOps
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import scala.concurrent.duration._

import scala.collection.mutable
import scala.collection.immutable


class PartitionInfo(val start: Long, val size: Long, val servers: List[ActorRef]) {

  override def toString: String = {
    s"$start - " + (size + start) + " -> " + servers
  }

  def inRange(key: Long): Boolean ={
    if ( start <= key &&  key < start + size )
      true
    else
      false
  }
}

sealed trait ShardingServiceAPI
case class RecordStats(stats: LoadStats) extends ShardingServiceAPI
case class RegisterClient(clientRef: ActorRef) extends ShardingServiceAPI
case object DeregisterClient extends ShardingServiceAPI
case object AnalyzeLoad extends ShardingServiceAPI
case object RequestUtilizations extends ShardingServiceAPI

class ShardingService(numCaches: Int, maxKey: Long) extends Actor {

  import context.dispatcher

  val log = Logging.getLogger(context.system, self)
  val maxThreshold = 1.25  // possibly change that to a different value
  val minThreshold = 0.75

  // Instantiate num caches, create initial uniform mapping
  private val caches = new mutable.MutableList[ActorRef]()
  for (i <- 0 until numCaches) {
    caches += context.actorOf(KVCache.props(i, self), "Cache" + i)
  }

  private val clients = new mutable.HashSet[ActorRef]()
  private var mapping = createInitialMapping()
  private var keysMoved: Long = 0

  private val rebalancer = new Rebalancer()
  pushUpdatedAssignment()

  private var analyzeLoadScheduler = context.system.scheduler.scheduleOnce(5 seconds, self, AnalyzeLoad)

  // we are going to append every time so list isn't best
  private val currentCacheLoads = new mutable.HashMap[ActorRef, mutable.MutableList[LoadStats]]()

  private def createInitialMapping(): mutable.MutableList[PartitionInfo] = {
    val initialSliceSize = maxKey / numCaches
    val m = new mutable.MutableList[PartitionInfo]()
    for (i <- caches.indices) {
      val c = caches.get(i).get
      val partitionStart = initialSliceSize * i
      var partitionSize = initialSliceSize
      if (i == numCaches - 1) {
        partitionSize = maxKey - partitionStart
      }
      m += new PartitionInfo(partitionStart, partitionSize, List(c))
    }
    m
  }

  private def pushUpdatedAssignment(): Unit = {
    val immutableMapping = mapping.toVector
    for (c <- clients) {
      c ! UpdateMapping(immutableMapping)
    }

    // construct per cache set of slices to send
    val perCacheAssignment = new mutable.HashMap[ActorRef, Set[PartitionInfo]]()

    for (p <- mapping) {
      for (cache <- p.servers) {
        val s = perCacheAssignment.get(cache)
        if (s.isDefined) {
          perCacheAssignment.put(cache, s.get + p)
        } else {
          perCacheAssignment.put(cache, Set(p))
        }
      }
    }

    for (c <- caches) {
      val assignment = perCacheAssignment.get(c)
      if (assignment.isDefined) {
        c ! UpdateAssignment(assignment.get)
      }
    }
  }

  override def receive: Receive = {
    case RecordStats(stats) => recordStats(stats, sender)
    case RegisterClient(clientRef) => registerClient(clientRef)
    case DeregisterClient => deregisterClient(sender)
    case AnalyzeLoad => analyzeLoad()
    case RequestUtilizations => requestUtilizations()
  }

  def registerClient(clientRef: ActorRef): Unit = {
    clients += clientRef
    sender ! UpdateMapping(mapping.toVector)
  }

  def deregisterClient(clientRef: ActorRef): Unit = {
    clients -= clientRef
  }

  def recordStats(stats: LoadStats, ref: ActorRef): Unit = {
    // add stats to map with key ref
    val currList = currentCacheLoads.get(ref)
    //log.info(stats.toString)
    if (currList.isDefined) {
      currList.get += stats
    } else {
      val l = new mutable.MutableList[LoadStats]()
      l += stats
      currentCacheLoads.put(ref, l)
    }
  }

  def analyzeLoad(): Unit = {
    val (utilizations, averageUtilization, maxCache, minCache) = rebalancer.computeAverageUtilizations(currentCacheLoads)
    //log.info(s"Average utilization is $averageUtilization")
    //log.info(s"Max load on $maxCache with " + utilizations.get(maxCache).get)
    var maxImbalance = 0d
    var minImbalance = 0d
    if (averageUtilization != 0) {
      //log.info(s"Max cache's utilization is " + utilizations.get(maxCache).get.utilization)
      //log.info(s"Min cache's utilization is " + utilizations.get(minCache).get.utilization)
      maxImbalance = utilizations.get(maxCache).get.utilization / averageUtilization
      minImbalance = utilizations.get(minCache).get.utilization / averageUtilization
    } else {
      //log.info("average util is 0")
      //log.info(utilizations.toString())
    }
    //log.info("maxImbalance is: " + maxImbalance.toString)
    //log.info("minImbalance is: " + minImbalance.toString)

    if(maxImbalance > maxThreshold || minImbalance < minThreshold){
      val t = rebalancer.rebalanceAssignment(utilizations)
      mapping = t._1
      keysMoved = t._2
      //println(s"keys moved is now $keysMoved")
      pushUpdatedAssignment()
    } else {
      // mapping stays the same but keysMoved should be zero
      keysMoved = 0
      //println(s"keys moved is now $keysMoved")
    }

    //log.info("updated mapping is: " + mapping.toString())
    currentCacheLoads.clear()
    analyzeLoadScheduler = context.system.scheduler.scheduleOnce(5 seconds, self, AnalyzeLoad)
  }

  def requestUtilizations(): Unit = {
    val mappingsMap = mapping.foldLeft(immutable.HashMap[ActorRef, Seq[(Long, Long)]]())((ans, entry) => {
      var newAns = ans
      for (s <- entry.servers) {
        if (ans.contains(s)) {
          newAns = newAns + (s -> (ans(s) :+ ((entry.start, entry.size))))
        } else
          newAns = newAns + (s -> (Seq((entry.start, entry.size))))
      }
      newAns
    })


    sender ! (immutable.HashMap() ++
        currentCacheLoads.map((entry) =>
            (entry._1.path.name, (entry._2.last.utilization, mappingsMap(entry._1)))), keysMoved / maxKey.asInstanceOf[Double])
  }
}

object ShardingService {
  def props(numCaches: Int, maxKey: Long): Props = {
    Props(classOf[ShardingService], numCaches, maxKey: Long)
  }
}
