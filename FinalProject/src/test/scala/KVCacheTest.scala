package slicer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{FlatSpec, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Await

class KVCacheTest extends TestKit(ActorSystem("KVCacheTest")) with ImplicitSender with FlatSpecLike {

  def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  println("test starting")

  "StatsAggregator" must "aggregate stats correctly" in {
    val aggregator = new StatsAggregator()

    val slice0 = new PartitionInfo(0, 4, null)
    val slice1 = new PartitionInfo(4, 4, null)
    val slice2 = new PartitionInfo(8, 4, null)
    val slice3 = new PartitionInfo(12, 4, null)

    // create sliceMap
    val sliceMap = mutable.HashMap[PartitionInfo, Long]((slice0, 0), (slice1, 0), (slice2, 0), (slice3, 0))
    // create ops queue
    val opsQueue = mutable.Queue[OperationInfo]()
    val startTime = System.currentTimeMillis()
    opsQueue.enqueue(new OperationInfo(0, 1))
    opsQueue.enqueue(new OperationInfo(1, 2))

    opsQueue.enqueue(new OperationInfo(4, 2))
    opsQueue.enqueue(new OperationInfo(4, 2))
    opsQueue.enqueue(new OperationInfo(6, 2))

    opsQueue.enqueue(new OperationInfo(10, 2))
    opsQueue.enqueue(new OperationInfo(11, 2))

    // check that aggregate loadstats is correct
    val stats = aggregator.aggregateStats(opsQueue, sliceMap, startTime)
    val pMap = stats.partitionMap
    assert(pMap.get(slice0).get == 3)
    assert(pMap.get(slice1).get == 6)
    assert(pMap.get(slice2).get == 4)
    assert(pMap.get(slice3).get == 0)

    val totalHeat = 13
    assert(stats.heat == totalHeat)
    assert(stats.utilization == totalHeat/1000d)
  }
}
