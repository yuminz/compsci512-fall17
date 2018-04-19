package rings

import akka.actor.{Actor, Props}

sealed trait KVStoreAPI
case class Put(key: BigInt, value: Any) extends KVStoreAPI
case class Get(key: BigInt) extends KVStoreAPI

/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */

class KVStore extends Actor {
  private val store = new scala.collection.mutable.HashMap[BigInt, Any]

  override def receive = {
    case Put(key, cell) =>
      sender ! store.put(key,cell)
    case Get(key) =>
      sender ! store.get(key)
  }
}

object KVStore {
  def props(): Props = {
     Props(classOf[KVStore])
  }
}
