package edu.rice.habanero.benchmarks.concsll

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SortedListAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SortedListAkkaActorBenchmark)
  }

  private final class SortedListAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SortedListConfig.parseArgs(args)
    }

    def printArgInfo() {
      SortedListConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {
      val numWorkers: Int = SortedListConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = SortedListConfig.NUM_MSGS_PER_WORKER

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new Master(numWorkers, numMessagesPerWorker, latch, ctx)
        ),
        "SortedList")
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case class MasterListMsg(master: ActorRef[Msg], sortedList: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(master, sortedList)
  }
  private case class WriteMessage(sender: ActorRef[Msg], value: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case class ContainsMessage(sender: ActorRef[Msg], value: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case class SizeMessage(sender: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case class ResultMessage(sender: ActorRef[Msg], value: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case object DoWorkMessage extends Msg with NoRefs
  private case object EndWorkMessage extends Msg with NoRefs

  private class Master(numWorkers: Int, numMessagesPerWorker: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val workers = new Array[ActorRef[Msg]](numWorkers)
    private final val sortedList = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SortedList(ctx)})
    private var numWorkersTerminated: Int = 0

    {
      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new Worker(i, numMessagesPerWorker, ctx)})
        workers(i) ! MasterListMsg(ctx.createRef(ctx.self, workers(i)), ctx.createRef(sortedList, workers(i)))
        workers(i) ! DoWorkMessage
        i += 1
      }
    }

    override def process(msg: Msg) {
      msg match {
        case EndWorkMessage =>
          numWorkersTerminated += 1
          if (numWorkersTerminated == numWorkers) {
            latch.countDown()
            exit()
          }
        case _ =>
      }
    }
  }

  private class Worker(id: Int, numMessagesPerWorker: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var master: ActorRef[Msg] = _
    private var sortedList: ActorRef[Msg] = _
    private final val writePercent = SortedListConfig.WRITE_PERCENTAGE
    private final val sizePercent = SortedListConfig.SIZE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new PseudoRandom(id + numMessagesPerWorker + writePercent + sizePercent)

    override def process(msg: Msg) {
      msg match {
        case MasterListMsg(master, sortedList) =>
          this.master = master
          this.sortedList = sortedList
          return

        case _ =>
      }
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.nextInt(100)
        if (anInt < sizePercent) {
          sortedList ! new SizeMessage(ctx.createRef(ctx.self, sortedList))
        } else if (anInt < (sizePercent + writePercent)) {
          sortedList ! new WriteMessage(ctx.createRef(ctx.self, sortedList), random.nextInt)
        } else {
          sortedList ! new ContainsMessage(ctx.createRef(ctx.self, sortedList), random.nextInt)
        }
      } else {
        master ! EndWorkMessage
        exit()
      }
    }
  }

  private class SortedList(ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private[concsll] final val dataList = new SortedLinkedList[Integer]

    override def process(msg: Msg) {
      msg match {
        case writeMessage: WriteMessage =>
          val value: Int = writeMessage.value
          dataList.add(value)
          val sender = writeMessage.sender
          sender ! new ResultMessage(ctx.createRef(ctx.self, sender), value)
        case containsMessage: ContainsMessage =>
          val value: Int = containsMessage.value
          val result: Int = if (dataList.contains(value)) 1 else 0
          val sender = containsMessage.sender
          sender ! new ResultMessage(ctx.createRef(ctx.self, sender), result)
        case readMessage: SizeMessage =>
          val value: Int = dataList.size
          val sender = readMessage.sender
          sender ! new ResultMessage(ctx.createRef(ctx.self, sender), value)
        case EndWorkMessage =>
          printf(BenchmarkRunner.argOutputFormat, "List Size", dataList.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }

}
