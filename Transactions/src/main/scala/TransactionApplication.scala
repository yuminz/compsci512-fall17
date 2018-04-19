package rings
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout

class TransactionApplication(val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, acquireWaitTime: Timeout, numKeys: Int) extends Actor {

  val log = Logging.getLogger(context.system, this)

  var transactionInProgress = false
  var readWritesPerformed = 0

  val generator = new scala.util.Random

  val client = new KVClient(storeServers, myNodeID, context.system, acquireWaitTime)

  var stats = new Stats

  override def receive: Receive = {
    case Command() => handleCommand()
  }


  def handleCommand(): Unit = {
    try {
      if (transactionInProgress) {
        val nextOperation = generator.nextInt(100)
        // TODO might want to play with the number of keys
        val nextKey = generator.nextInt(numKeys)
        if (readWritesPerformed == 0) {
          // we always choose to read/write if this is a new transaction, otherwise we could potentially commit
          // an empty transaction
          if (nextOperation < 50) {
            client.read(nextKey)
          } else {
            // the value written to the KVStores is irrelevant to the stats we are collecting
            client.write(nextKey, "newValue")
          }
          readWritesPerformed += 1
        } else {
          // choose between read/write/commit/abort
          if (nextOperation < 30) {
            client.read(nextKey)
            readWritesPerformed += 1
          } else if (nextOperation < 80) {
            client.write(nextKey, "newValue")
            readWritesPerformed += 1
          } else if (nextOperation < 97) {
            client.commit()
            stats.transactionsSucceeded += 1
            resetStatus()
          } else {
            client.abort()
            stats.transactionsAbortCalled += 1
            resetStatus()
          }
        }
      } else {
        client.begin()
        stats.transactionsBegun += 1
        transactionInProgress = true
      }
    } catch {
      case ex: TransactionAbortedException => {
        // commit procedure failed
        stats.transactionsAbortedDuringCommit += 1
        resetStatus()
      }
      case ex: TransactionAbortedEarlyException => {
        // record stat for lost lease
        stats.transactionsAbortedEarly += 1
        resetStatus()
      }
      case ex: AcquireTimeoutException => {
        // record stat for not able to acquire lock in time
        stats.transactionsAbortedUnableToAcquire += 1
        resetStatus()
      }
    }
    incoming()
  }

  private def resetStatus(): Unit = {
    transactionInProgress = false
    readWritesPerformed = 0
  }

  private def incoming() = {
    stats.messages += 1
    if (stats.messages >= burstSize) {
      sender ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }


}

object TransactionApplication {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, acquireWaitTime: Timeout, numKeys: Int): Props = {
    Props(classOf[TransactionApplication], myNodeID, numNodes, storeServers, burstSize, acquireWaitTime, numKeys)
  }
}

