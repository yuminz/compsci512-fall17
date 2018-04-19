package rings

import scala.collection.mutable.Set
import scala.util.Random

sealed trait CommandType
object CommandType {
  case object Acquire extends CommandType
  case object Release extends CommandType
}

trait Picker {
  val generator_ = new scala.util.Random
  def generator = generator_

  def pickCommand() : CommandType = {
    val sample = generator.nextInt(100)
    if (sample < 50) {
      CommandType.Acquire
    } else {
      CommandType.Release
    }
  }

  def pickLock(availLocks : Set[String]) : String
}

class UniformPicker extends Picker {
  def pickLock(availLocks : Set[String]) : String = {
    val locks = availLocks.toVector.sorted
    locks(super.generator.nextInt(locks.size))
  }
}



