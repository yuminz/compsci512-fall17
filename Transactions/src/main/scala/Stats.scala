package rings

import scala.collection.mutable

class Stats {
  var messages: Int = 0
  var transactionsBegun: Int = 0
  var transactionsSucceeded: Int = 0
  var transactionsAbortCalled: Int = 0
  var transactionsAbortedEarly: Int = 0
  var transactionsAbortedDuringCommit: Int = 0
  var transactionsAbortedUnableToAcquire: Int = 0

  def += (right: Stats): Stats = {
    messages += right.messages
    transactionsBegun += right.transactionsBegun
    transactionsSucceeded += right.transactionsSucceeded
    transactionsAbortCalled += right.transactionsAbortCalled
    transactionsAbortedEarly += right.transactionsAbortedEarly
    transactionsAbortedUnableToAcquire += right.transactionsAbortedUnableToAcquire
    transactionsAbortedDuringCommit += right.transactionsAbortedDuringCommit
	  this
  }

  override def toString(): String = {
    s"Stats msgsFromMaster=$messages transactionsBegun=$transactionsBegun transactionsSucceeded=$transactionsSucceeded transactionsAbortCalled=$transactionsAbortCalled transactionsUnableToAcquireLock=$transactionsAbortedUnableToAcquire transactionsAbortedEarly=$transactionsAbortedEarly transactionsAbortedDuringCommit=$transactionsAbortedDuringCommit \n"
  }
}
