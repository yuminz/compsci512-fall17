package rings
import akka.actor.{Actor, DeadLetter, Props}
import akka.event.Logging


// We created this class for debugging purposes
class DeadLetterListener extends Actor {
  val log = Logging(context.system, this)
  def receive = {
    case DeadLetter(msg, from, to) => {
      log.info("dead letter from: " + from)
      log.info("dead letter to: " + to)
      log.info("dead letter contents: " + msg)
    }
  }
}

object DeadLetterListener {
  def props(): Props = {
    Props(classOf[DeadLetterListener])
  }
}
