package edu.rice.habanero.benchmarks.fjcreate

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
object ForkJoinAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinAkkaActorBenchmark)
  }

  private final class ForkJoinAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ForkJoinConfig.parseArgs(args)
    }

    def printArgInfo() {
      ForkJoinConfig.printArgs()
    }

    private var system: ActorSystem[Msg.type] = _
    def runIteration() {

      val latch = new CountDownLatch(ForkJoinConfig.N)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot[Msg.type](ctx =>
          new Master(latch, ctx)),
        "ForkJoin")

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  case object Msg extends Message with NoRefs

  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg.type]) extends GCActor[Msg.type](ctx) {
    {
      var i = 0
      while (i < ForkJoinConfig.N) {
        val fjRunner = ctx.spawnAnonymous(Behaviors.setup[Msg.type](ctx => new ForkJoinActor(latch, ctx)))
        fjRunner ! Msg
        i += 1
      }
    }

    def process(msg: Msg.type) = ()
  }

  private class ForkJoinActor(latch: CountDownLatch, ctx: ActorContext[Msg.type])
    extends GCActor[Msg.type](ctx) {
    override def process(msg: Msg.type) {
      ForkJoinConfig.performComputation(37.2)
      latch.countDown()
      exit()
    }
  }

}
