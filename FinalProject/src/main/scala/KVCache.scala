package slicer

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.Logging

import scala.concurrent.duration._
import scala.collection.mutable
import scala.language.postfixOps

sealed trait KVCacheAPI
case class Operation(key: Long, duration: Int) extends KVCacheAPI
case object PushStatsTimeout extends KVCacheAPI
case class UpdateAssignment(slices: Set[PartitionInfo]) extends KVCacheAPI

class OperationInfo (val key: Long, val duration: Int){
  var timestamp = System.currentTimeMillis()
}

// TODO make 1000 a variable for frequency
class StatsAggregator {
  def aggregateStats(allOps: mutable.Queue[OperationInfo], sliceMap: mutable.Map[PartitionInfo, Long], startStatsTime: Long): LoadStats = {
    var totalDuration: Double = 0
    for (s <- sliceMap.keySet){
      sliceMap.put(s, 0)
    }

    if (!allOps.isEmpty) {
      var nextOp = allOps.front
      var currentTime = startStatsTime
      val endTime = startStatsTime + 1000
      var partial = false

      while ((nextOp.timestamp < endTime) && (currentTime < endTime)){
        if (partial){
          // we need duration to decrease each time to represent remaining duration
          val durationLeft = nextOp.duration - (currentTime - nextOp.timestamp)
          if (durationLeft > 1000){
            for (s <- sliceMap.keySet){
              if (s.inRange(nextOp.key)){
                val value = sliceMap.get(s).get + 1000
                sliceMap.put(s, value)
              }
            }

            totalDuration += 1000
            currentTime = endTime
          }else{
            partial = false
            currentTime += durationLeft
            totalDuration += durationLeft

            for (s <- sliceMap.keySet){
              if (s.inRange(nextOp.key)){
                val value = sliceMap.get(s).get + durationLeft
                sliceMap.put(s, value)
              }
            }

            allOps.dequeue()
            if (allOps.isEmpty){
              currentTime = endTime
            }
            else {
              nextOp = allOps.front
            }
          }

        }
        else {
          if (nextOp.timestamp > currentTime) {
            currentTime = nextOp.timestamp
          }

          if (currentTime + nextOp.duration > endTime) {
            partial = true
            totalDuration += (endTime - currentTime)
            nextOp.timestamp = currentTime
            for (s <- sliceMap.keySet){
              if (s.inRange(nextOp.key)){
                val value = sliceMap.get(s).get + (endTime - currentTime)
                sliceMap.put(s, value)
              }
            }
            currentTime = endTime

          } else {
            currentTime = currentTime + nextOp.duration
            totalDuration += nextOp.duration

            for (s <- sliceMap.keySet){
              if (s.inRange(nextOp.key)){
                val value = sliceMap.get(s).get + nextOp.duration
                sliceMap.put(s, value)
              }
            }


            allOps.dequeue()
            if (allOps.isEmpty){
              currentTime = endTime
            }
            else {
              nextOp = allOps.front
            }
          }
        }
      }
    }
    val utilization : Double = totalDuration / 1000.0
    new LoadStats(utilization, totalDuration.asInstanceOf[Long], sliceMap.toMap)
  }
}

//TODO: can get rid of utilization but instead do heat/1000.0
// define heat in terms of partitionMap entries
// have average method that takes in a time interval and returns a new load stats with averaged over that interval
class LoadStats (var utilization: Double, var heat: Long, val partitionMap: Map[PartitionInfo, Long]) {

  override def toString: String = {
   "total heat: " + heat + " over: " + partitionMap.toString()
  }

  def + (other: LoadStats): LoadStats = {
    val util = utilization + other.utilization
    val totalHeat = heat + other.heat
    val newMap = partitionMap ++ other.partitionMap.map{ case (k: PartitionInfo, v: Long) => (k, partitionMap.get(k).getOrElse(0l) + v)}
    new LoadStats(util, totalHeat, newMap)
  }
}

// need an ID and service
class KVCache(val myID: Int, slicerService: ActorRef) extends Actor{
  import context.dispatcher

  val log = Logging.getLogger(context.system, self)
  val allOps = mutable.Queue[OperationInfo]()
  var pushStatScheduler: Option[Cancellable] = Some(context.system.scheduler.scheduleOnce(1 second, self, PushStatsTimeout))
  var startStatsTime = System.currentTimeMillis()
  var sliceMap: mutable.HashMap[PartitionInfo, Long] = new mutable.HashMap[PartitionInfo, Long]()

  val aggregator = new StatsAggregator()

  override def receive = {
    case Operation(key, duration) => {
      allOps.enqueue(new OperationInfo(key, duration))
    }
    case PushStatsTimeout => pushStats()
    case UpdateAssignment(slices) => updateMapping(slices)

  }

  // clear out all the operations from old mapping
  def updateMapping(slices: Set[PartitionInfo]): Unit = {
    sliceMap.clear()
    allOps.clear()
    startStatsTime = System.currentTimeMillis()
    if (pushStatScheduler.isDefined){
      pushStatScheduler.get.cancel()
    }
    pushStatScheduler = Some(context.system.scheduler.scheduleOnce(1 second, self, PushStatsTimeout))
    for (s <- slices){
      sliceMap.put(s, 0)
    }
  }



  def pushStats() : Unit = {
    val myStats = aggregator.aggregateStats(allOps, sliceMap, startStatsTime)
    //log.info("total heat: " + totalDuration + " and slice map: " + sliceMap.toString())
    slicerService ! RecordStats(myStats)
    startStatsTime += 1000
    pushStatScheduler = Some(context.system.scheduler.scheduleOnce(1 second, self, PushStatsTimeout))
  }
}

object KVCache {
  def props(myID: Int, slicerService: ActorRef): Props = {
    Props(classOf[KVCache], myID, slicerService)
  }
}


