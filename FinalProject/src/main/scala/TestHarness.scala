package slicer

import akka.actor.{ActorSystem, ActorRef}
import akka.util.Timeout

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

object TestHarness {
  var appCount = 0

  implicit val timeout = Timeout(60 seconds)
  var system = ActorSystem("Slicer")

  var shardingService : ActorRef = system.actorOf(ShardingService.props(4, 19), "ShardingService")
  val dataGUI = new DataGUI(shardingService)(timeout)

  val loaders = ListBuffer[ActorRef]()

  @inline def defined(line: String) = {
    line != null
  }

  def main(args: Array[String]): Unit = {
    dataGUI.start

    var continueReading = true;
    Iterator
        .continually(scala.io.StdIn.readLine)
        .takeWhile(defined(_) && continueReading)
        .foreach(line => {
          println(s"got command $line")
          val command = line.split("\\s+")
          command(0) match {
            case "add" => handleAdd(command.slice(1, command.size))
            case "list" => handleList
            case "remove" => handleRemove(command.slice(1, command.size))
            case "restart" => handleRestart(command.slice(1, command.size))

            case "help" => printHelp()

            case "end" => continueReading = false
            case _ => printHelp()
          }
        })

    dataGUI.stop
    system.shutdown()
  }

  def handleAdd(args: Array[String]) {
    var pick : Picker = null
    val interval = args(0).toInt
    val operationTime = args(1).toInt
    val cmdtype = args(2)
    cmdtype match {
      case "range" => pick = new RangePicker(args(3).toInt, args(4).toInt)
      case "uniform" => pick = new UniformPicker(
        args.slice(3, args.size).map(_.toLong).toSet[Long])
      case _ => return
    }
    println("Creating new client application...")

    appCount += 1
    val newApp = system.actorOf(ClassApplication.props(pick, interval, operationTime, shardingService), "App" + appCount)
    loaders += newApp
    newApp ! Start()

    println(s"Your new client has index: ${loaders.size-1}")
    println("Beware this changes if you delete a client!")
  }

  def handleList = {
    (0 to loaders.size - 1).foreach(x => loaders(x) ! PrintRequest(s"$x:"))
  }

  def handleRemove(args: Array[String]) {
    if (args.size != 1) {
      println("Wrong number of arguments!")
    } else {
      val ind = args(0).toInt
      loaders(ind) ! Stop()
      loaders.remove(ind)
    }
  }

  def handleRestart(args: Array[String]) {
    if (args.size != 2) {
      println("Wrong number of arguments!")
    } else {
      loaders.foreach(l => l ! Stop())
      loaders.clear
      system.shutdown
      system = ActorSystem("Slicer")
      shardingService = system.actorOf(ShardingService.props(args(0).toInt, args(1).toInt), "ShardingService")
      dataGUI.updateService(shardingService)
    }
  }

  def printHelp() {
    val helpString = "Commands:\n" +
                     "add interval op_time picker [picker args...]  - add a new client with the specified\n" +
                     "                                        picker (see below) to send requests\n" +
                     "                                        every interval seconds.\n" +
                     "list                                  - retrieve indexes and descriptors for\n" +
                     "                                        all clients\n" +
                     "remove index                          - remove client at the specified index\n" +
                     "restart ncache keyspace               - restart simulation with ncache caches\n" +
                     "                                        and the given keyspace\n" +
                     "\n" +
                     "Available pickers:\n" +
                     "range begin end                       - send all keys from begin to end\n" +
                     "uniform [keys...]                     - specify all keys to use\n"
    println(s"$helpString")
  }
}
