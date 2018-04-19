package slicer

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{FlatSpec, FlatSpecLike}

class RebalancerTest extends TestKit(ActorSystem("GroupServiceTest")) with ImplicitSender with FlatSpecLike {

  def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  println("test starting")

  "Binary search" must "find a partition with heat in the range desired" in {

    val slicer = new Rebalancer()
    val p = new PartitionInfo(0, 10, null)
    var partitions = Vector[(PartitionInfo, Long)]((p, 100l), (p, 300l), (p, 400l), (p, 450), (p, 500), (p, 550), (p, 600))
    var lower = 450
    var upper = 550
    val i = slicer.binarySearch(partitions, lower, upper).get
    assert((i == 3) || (i == 4))

    partitions = Vector[(PartitionInfo, Long)]((p, 0l), (p, 1l), (p, 449l), (p, 550), (p, 600), (p, 800))
    val j  = slicer.binarySearch(partitions, lower, upper).get
    assert(j == 2)

    partitions = Vector[(PartitionInfo, Long)]((p, 100l), (p, 200l), (p, 300))
    val k = slicer.binarySearch(partitions, lower, upper).get
    assert(k == 2)

    /*
    sorted slices: Vector((1 - 2 -> Actor[akka://Slicer/user/SlicerService/Cache0#-2113294098],1), (6 - 7 -> Actor[akka://Slicer/user/SlicerService/Cache2#-1564001941],40))
[info] upper bound: 1
     */

    partitions = Vector[(PartitionInfo, Long)]((p, 1), (p, 40))
    lower = 1
    upper = 1
    val check = slicer.binarySearch(partitions, lower, upper).get
    assert(check == 0)
  }
}