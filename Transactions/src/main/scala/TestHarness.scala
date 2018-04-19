package rings

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

object TestHarness {
  val system = ActorSystem("Rings")
  implicit val timeout = Timeout(60 seconds)
  val numNodes = 3
  val burstSize = 10
  val opsPerNode = 50

  val numKeys = 100
  val acquireWaitTime = Timeout(5 seconds)
  val leaseTime = Timeout(1 seconds)
  val partitionFrequency = Timeout(5 seconds)

  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numNodes, burstSize, partitionFrequency, leaseTime, acquireWaitTime, numKeys)

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
