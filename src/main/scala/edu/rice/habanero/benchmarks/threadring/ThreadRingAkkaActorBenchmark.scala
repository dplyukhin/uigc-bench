package edu.rice.habanero.benchmarks.threadring

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{GCActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThreadRingAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThreadRingAkkaActorBenchmark)
  }

  private final class ThreadRingAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThreadRingConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThreadRingConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot[Msg](ctx =>
          new Master(latch, ctx)),
        "ThreadRing")

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  case class PingMessage (val pingsLeft: Int) extends Msg with NoRefs {
    def hasNext: Boolean = pingsLeft > 0
    def next () = PingMessage(pingsLeft - 1)
  }
  case class DataMessage (val data: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(data)
  }
  case class ExitMessage (val exitsLeft: Int) extends Msg with NoRefs {
    def hasNext: Boolean = exitsLeft > 0
    def next() = ExitMessage(exitsLeft - 1)
  }

  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {
      val numActorsInRing = ThreadRingConfig.N
      val ringActors = Array.tabulate[ActorRef[Msg]](numActorsInRing)(i => {
        val loopActor = ctx.spawnAnonymous(
          Behaviors.setup[Msg](ctx => new ThreadRingActor(i, numActorsInRing, latch, ctx)))
        loopActor
      })

      for ((loopActor, i) <- ringActors.view.zipWithIndex) {
        val nextActor = ringActors((i + 1) % numActorsInRing)
        loopActor ! DataMessage(ctx.createRef(nextActor, loopActor))
      }

      ringActors(0) ! PingMessage(ThreadRingConfig.R)
    }

    def process(msg: Msg) = ()
  }

  private class ThreadRingActor(id: Int, numActorsInRing: Int, latch: CountDownLatch, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = null

    override def process(msg: Msg) {

      msg match {

        case pm: PingMessage =>

          if (pm.hasNext) {
            nextActor ! pm.next()
          } else {
            latch.countDown()
            nextActor ! new ExitMessage(numActorsInRing)
          }

        case em: ExitMessage =>

          if (em.hasNext) {
            nextActor ! em.next()
          }
          exit()

        case dm: DataMessage =>

          nextActor = dm.data
      }
    }
  }

}
