package rings

import scala.collection.mutable.Set

sealed trait CommandType
object CommandType {
  case object Join extends CommandType
  case object SendMsg extends CommandType
  case object Leave extends CommandType
}

trait Picker {
  val generator_ = new scala.util.Random
  def generator = generator_

  def pickCommand() : CommandType = {
    val sample = generator.nextInt(100)
    if (sample < 35) {
      CommandType.Join
    } else if (sample < 90) {
      CommandType.SendMsg
    } else {
      CommandType.Leave
    }
  }

  def pickGroup(availGroups : Set[BigInt]) : BigInt
}

class UniformPicker extends Picker {
  def pickGroup(availGroups : Set[BigInt]) : BigInt = {
    val groups = availGroups.toVector.sorted
    groups(super.generator.nextInt(groups.size))
  }
}

class SkewedPicker extends Picker {
  def pickGroup(availGroups : Set[BigInt]) : BigInt = {
    val groups = availGroups.toVector.sorted
    if (generator.nextInt(100) < 70) {
      groups(0)
    } else {
      groups(super.generator.nextInt(groups.size))
    }
  }
}



