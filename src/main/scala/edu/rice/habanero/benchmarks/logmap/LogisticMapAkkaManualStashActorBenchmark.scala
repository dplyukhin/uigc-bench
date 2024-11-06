package edu.rice.habanero.benchmarks.logmap

import java.util
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.logmap.LogisticMapConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LogisticMapAkkaManualStashActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new LogisticMapAkkaManualStashActorBenchmark)
  }

  private final class LogisticMapAkkaManualStashActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      LogisticMapConfig.parseArgs(args)
    }

    def printArgInfo() {
      LogisticMapConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new Master(latch, ctx)
        ),
        "LogisticMap")
      system ! StartMessage
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case class MasterComputerMsg(master: ActorRef[Msg], computer: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(master, computer)
  }
  private case object StartMessage extends Msg with NoRefs
  private case object StopMessage extends Msg with NoRefs
  private case object NextTermMessage extends Msg with NoRefs
  private case object GetTermMessage extends Msg with NoRefs
  private case class ComputeMessage(sender: ActorRef[Msg], term: Double) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }
  private case class ResultMessage(term: Double) extends Msg with NoRefs

  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val numComputers: Int = LogisticMapConfig.numSeries
    private final val computers = Array.tabulate[ActorRef[Msg]](numComputers)(i => {
      val rate = LogisticMapConfig.startRate + (i * LogisticMapConfig.increment)
      ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new RateComputer(rate, ctx)})
    })

    private final val numWorkers: Int = LogisticMapConfig.numSeries
    private final val workers = Array.tabulate[ActorRef[Msg]](numWorkers)(i => {
      val rateComputer = computers(i % numComputers)
      val startTerm = i * LogisticMapConfig.increment
      val a = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SeriesWorker(i, startTerm, ctx)})
      a ! MasterComputerMsg(ctx.createRef(ctx.self, a), ctx.createRef(rateComputer, a))
      a
    })

    private var numWorkRequested: Int = 0
    private var numWorkReceived: Int = 0
    private var termsSum: Double = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case StartMessage =>

          var i: Int = 0
          while (i < LogisticMapConfig.numTerms) {
            // request each worker to compute the next term
            workers.foreach(loopWorker => {
              loopWorker ! NextTermMessage
            })
            i += 1
          }

          // workers should stop after all items have been computed
          workers.foreach(loopWorker => {
            loopWorker ! GetTermMessage
            numWorkRequested += 1
          })

        case rm: ResultMessage =>

          termsSum += rm.term
          numWorkReceived += 1

          if (numWorkRequested == numWorkReceived) {

            println("Terms sum: " + termsSum)
            latch.countDown()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SeriesWorker(id: Int, startTerm: Double, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var master: ActorRef[Msg] = _
    private var computer: ActorRef[Msg] = _
    private final val curTerm = Array.tabulate[Double](1)(i => startTerm)

    private var inReplyMode = false
    private val stashedMessages = new util.LinkedList[Msg]()

    override def process(theMsg: Msg) {
      theMsg match {
        case MasterComputerMsg(x,y) =>
          this.master = x
          this.computer = y
          return
        case _ =>
      }

      if (inReplyMode) {

        theMsg match {

          case resultMessage: ResultMessage =>

            inReplyMode = false
            curTerm(0) = resultMessage.term

          case message =>

            stashedMessages.add(message)
        }

      } else {

        // process the message
        theMsg match {

          case NextTermMessage =>

            val sender = ctx.self
            computer ! new ComputeMessage(ctx.createRef(sender, computer), curTerm(0))
            inReplyMode = true

          case message: GetTermMessage.type =>

            // do not reply to master if stash is not empty
            if (stashedMessages.isEmpty) {
              master ! new ResultMessage(curTerm(0))
            } else {
              stashedMessages.add(message)
            }

          case StopMessage =>

            exit()

          case message =>
            val ex = new IllegalArgumentException("Unsupported message: " + message)
            ex.printStackTrace(System.err)
        }
      }

      // recycle stashed messages
      if (!inReplyMode && !stashedMessages.isEmpty) {
        val newMsg = stashedMessages.remove(0)
        ctx.self ! newMsg
      }
    }
  }

  private class RateComputer(rate: Double, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    override def process(theMsg: Msg) {
      theMsg match {
        case computeMessage: ComputeMessage =>

          val result = computeNextTerm(computeMessage.term, rate)
          val resultMessage = new ResultMessage(result)
          computeMessage.sender ! resultMessage

        case StopMessage =>

          exit()

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
