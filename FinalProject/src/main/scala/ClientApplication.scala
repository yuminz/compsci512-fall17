package slicer

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.Logging

import scala.collection.mutable.HashMap
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

sealed trait ClassApplicationAPI
case class PrintRequest(prefix: String) extends ClassApplicationAPI
case class Start() extends ClassApplicationAPI
case class Stop() extends ClassApplicationAPI

class ClassApplication(private val _picker: Picker,
                       private val _interval: Int,
                       private val _opTime: Int,
                       private val _service: ActorRef) extends Actor {
  import context.dispatcher

  val log = Logging.getLogger(context.system, self)

  val client = new KVClient(_service, this)
  var task : Cancellable = null

  override def receive = {
    case PrintRequest(prefix) => println(s"$prefix ${_picker.name} every ${_interval}")
    case Start() => start
    case Stop() => stop
  }

  def start : Unit = {
    if (task == null) {
      task = context.system.scheduler.schedule(
          new DurationInt(0).seconds,
          new FiniteDuration(_interval, MILLISECONDS),
          new Runnable {
            def run : Unit = {
              val key = _picker.nextKey
              client.execute(key, _opTime)
            }
          })
    }
  }

  def stop : Unit = {
    if (task != null && !task.isCancelled) {
      task.cancel
      task = null
    }
    client.close()
  }
}

object ClassApplication {
  def props(picker: Picker, interval: Int, opTime: Int, service: ActorRef) : Props = {
    Props(classOf[ClassApplication], picker, interval, opTime, service)
  }
}
