package edu.rice.habanero.benchmarks.trapezoid

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
object TrapezoidalAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new TrapezoidalAkkaActorBenchmark)
  }

  private final class TrapezoidalAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      TrapezoidalConfig.parseArgs(args)
    }

    def printArgInfo() {
      TrapezoidalConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val numWorkers: Int = TrapezoidalConfig.W
      val precision: Double = (TrapezoidalConfig.R - TrapezoidalConfig.L) / TrapezoidalConfig.N

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new Master(numWorkers, latch, ctx)
        ),
        "Trapezoidal")
      system ! new WorkMessage(TrapezoidalConfig.L, TrapezoidalConfig.R, precision)
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
  private case class WorkMessage(l: Double, r: Double, h: Double) extends Msg with NoRefs
  private case class ResultMessage(result: Double, workerId: Int) extends Msg with NoRefs

  private class Master(numWorkers: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val workers = Array.tabulate[ActorRef[Msg]](numWorkers)(i => {
      val a = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new Worker(i, ctx)})
      a ! Rfmsg(ctx.createRef(ctx.self, a))
      a
    })
    private var numTermsReceived: Int = 0
    private var resultArea: Double = 0.0

    override def process(msg: Msg) {
      msg match {
        case rm: ResultMessage =>

          numTermsReceived += 1
          resultArea += rm.result

          if (numTermsReceived == numWorkers) {
            println("  Area: " + resultArea)
            latch.countDown()
          }

        case wm: WorkMessage =>

          val workerRange: Double = (wm.r - wm.l) / numWorkers
          for ((loopWorker, i) <- workers.view.zipWithIndex) {

            val wl = (workerRange * i) + wm.l
            val wr = wl + workerRange

            loopWorker ! new WorkMessage(wl, wr, wm.h)
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class Worker(val id: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    private var master: ActorRef[Msg] = _

    override def process(msg: Msg) {
      msg match {
        case rm: Rfmsg => this.master = rm.actor
        case wm: WorkMessage =>

          val n = ((wm.r - wm.l) / wm.h)
          var accumArea = 0.0

          var i = 0
          while (i < n) {
            val lx = (i * wm.h) + wm.l
            val rx = lx + wm.h

            val ly = TrapezoidalConfig.fx(lx)
            val ry = TrapezoidalConfig.fx(rx)

            val area = 0.5 * (ly + ry) * wm.h
            accumArea += area

            i += 1
          }

          master ! new ResultMessage(accumArea, id)
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
