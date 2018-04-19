package rings

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.FlatSpecLike

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

trait MockGroupServerAPI
case class GetMessageCount() extends MockGroupServer

class MockGroupServer extends Actor {
  var msgCount = 0

  def receive = {
    case GetMessageCount => sender ! msgCount
    case JoinGroup(groupId, memberId) => msgCount += 1
  }
}

class LoadTestPicker extends Picker {
  var nextGroup: Int = 0

  override def pickCommand(): CommandType = {
    CommandType.Join
  }

  override def pickGroup(availGroups: mutable.Set[BigInt]): BigInt = {
    nextGroup += 1
    nextGroup % 4
  }
}

class ConcurrentJoinTestPicker extends Picker {
  override def pickCommand(): CommandType = {
    CommandType.Join
  }

  override def pickGroup(availGroups: mutable.Set[BigInt]): BigInt = {
    0
  }
}

class GroupServiceTest extends TestKit(ActorSystem("GroupServiceTest")) with ImplicitSender with FlatSpecLike {

  def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Group service" must "send an ack to sender of join or leave" in {

    val kv = system.actorOf(KVStore.props())
    val a = system.actorOf(GroupServer.props(0, 1, Seq(kv), 10, new ConcurrentJoinTestPicker()))

    a ! JoinGroup(0, 0)

    expectMsg(JoinAck(0, true))

    a ! LeaveGroup(0, 0)

    expectMsg(LeaveAck(0, true))
  }

  "Group service" must "balance load of managing groups equally around GroupServers" in {
    // create seq of GroupServer probes
    val mockGroupServers = for {
      i <- 0 until 4
    } yield system.actorOf(Props(classOf[MockGroupServer]))

    // pass seq to GroupServer under test
    val underTest = system.actorOf(GroupServer.props(0, 4, mockGroupServers, 10, new LoadTestPicker()))

    underTest ! View(mockGroupServers)

    for (i <- 0 until 8) {
      underTest ! Command()
    }


    implicit val timeout = Timeout(60 seconds)

    val msgCounts = for {
      m <- mockGroupServers
    } yield {
      val future = ask(mockGroupServers.head, GetMessageCount)
      val count = Await.result(future, timeout.duration)
      count
    }

    val firstCount = msgCounts.head
    assert(msgCounts.count(_ == firstCount) == mockGroupServers.size)
  }

  "GroupService" must "allow for multiple join requests for the same group to not be lost" in {
    // create real KVStores
    val numServers = 4
    val kvStores = for {
      i <- 0 until numServers
    } yield system.actorOf(KVStore.props())

    // create a few real actors
    val groupServers = for {
      i <- 0 until numServers
    } yield system.actorOf(GroupServer.props(i, numServers, kvStores, 10, new ConcurrentJoinTestPicker()))

    for (g <- groupServers) {
      g ! View(groupServers)
    }

    for (g <- groupServers) {
      g ! Command()
    }

    // We must give each of the group servers time to suceessfully add themselves to the KVStore
    Thread.sleep(1000)

    implicit val timeout = Timeout(60 seconds)
    // use given gKVStore - check that every actor was added to that group
    val future = ask(kvStores.head, Get(0))
    val set = Await.result(future, 60 seconds).asInstanceOf[Option[mutable.Set[Int]]].get
    assert(set.size == numServers)
  }
}