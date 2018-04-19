package rings

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

object TestHarness {
  val system = ActorSystem("Rings")
  implicit val timeout = Timeout(90 seconds)
  val numNodes = 5
  val burstSize = 5
  val opsPerNode = 15

  val leaseTime = Timeout(5 seconds)
  val lockWaitTime = Timeout(5 seconds)
  // If you want to turn off partitions, you can set partitionFrequency to a time greater than timeout
  val partitionFrequency = Timeout(20 seconds)
  val numLocks = 30


  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numNodes, burstSize, leaseTime, numLocks, lockWaitTime, partitionFrequency)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val s = System.currentTimeMillis
    runUntilDone
    val runtime = System.currentTimeMillis - s
    val throughput = (opsPerNode * numNodes)/runtime
    println(s"Done in $runtime ms ($throughput Kops/sec)")
    system.shutdown()
  }

  def runUntilDone() = {
    master ! Start(opsPerNode)
    val future = ask(master, Join()).mapTo[Stats]
    val done = Await.result(future, 60 seconds)
  }
}
