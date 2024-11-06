package edu.rice.habanero.benchmarks.piprecision

import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicInteger
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PiPrecisionAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PiPrecisionAkkaActorBenchmark)
  }

  private final class PiPrecisionAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PiPrecisionConfig.parseArgs(args)
    }

    def printArgInfo() {
      PiPrecisionConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {
      val numWorkers: Int = PiPrecisionConfig.NUM_WORKERS
      val precision: Int = PiPrecisionConfig.PRECISION

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new Master(numWorkers, precision, latch, ctx)
        ),
        "PiPrecision")
      system ! StartMessage
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message

  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  private case object StartMessage extends Msg with NoRefs
  private case object StopMessage extends Msg with NoRefs
  private case class WorkMessage(scale: Int, term: Int) extends Msg with NoRefs
  private case class ResultMessage(result: BigDecimal, workerId: Int) extends Msg with NoRefs

  private class Master(numWorkers: Int, scale: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val workers = Array.tabulate[ActorRef[Msg]](numWorkers)(i => {
      val a = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new Worker(i, ctx) })
      a ! Rfmsg(ctx.createRef(ctx.self, a))
      a
    })
    private var result: BigDecimal = BigDecimal.ZERO
    private final val tolerance = BigDecimal.ONE.movePointLeft(scale)
    private final val numWorkersTerminated: AtomicInteger = new AtomicInteger(0)
    private var numTermsRequested: Int = 0
    private var numTermsReceived: Int = 0
    private var stopRequests: Boolean = false


    /**
     * Generates work for the given worker
     *
     * @param workerId the id of te worker to send work
     */
    private def generateWork(workerId: Int) {
      val wm: WorkMessage = new WorkMessage(scale, numTermsRequested)
      workers(workerId) ! wm
      numTermsRequested += 1
    }

    def requestWorkersToExit() {
      workers.foreach(loopWorker => {
        loopWorker ! StopMessage
      })
    }

    override def process(msg: Msg) {
      msg match {
        case rm: ResultMessage =>
          numTermsReceived += 1
          result = result.add(rm.result)
          if (rm.result.compareTo(tolerance) <= 0) {
            stopRequests = true
          }
          if (!stopRequests) {
            generateWork(rm.workerId)
          }
          if (numTermsReceived == numTermsRequested) {
            latch.countDown()
          }
        case StopMessage =>
          val numTerminated: Int = numWorkersTerminated.incrementAndGet
          if (numTerminated == numWorkers) {
            exit()
          }
        case StartMessage =>
          var t: Int = 0
          while (t < Math.min(scale, 10 * numWorkers)) {
            generateWork(t % numWorkers)
            t += 1
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }

    def getResult: String = {
      result.toPlainString
    }
  }

  private class Worker(id: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    var master: ActorRef[Msg] = _
    override def process(msg: Msg) {
      msg match {
        case Rfmsg(master) => this.master = master
        case StopMessage =>
          master ! StopMessage
          exit()
        case wm: WorkMessage =>
          val result: BigDecimal = PiPrecisionConfig.calculateBbpTerm(wm.scale, wm.term)
          master ! new ResultMessage(result, id)
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
