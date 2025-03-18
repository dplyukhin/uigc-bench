package edu.rice.habanero.benchmarks.fjthrput

import edu.rice.habanero.actors.{AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThroughputAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThroughputAkkaActorBenchmark)
  }

  private final class ThroughputAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThroughputConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThroughputConfig.printArgs()
    }

    private var system: ActorSystem[Msg.type] = _
    def runIteration() {

      val latch = new CountDownLatch(ThroughputConfig.A)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot[Msg.type](ctx =>
          new Master(latch, ctx)),
        "Throughput")

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  case object Msg extends Message with NoRefs

  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg.type]) extends GCActor[Msg.type](ctx) {
    {
      val actors = Array.tabulate[ActorRef[Msg.type]](ThroughputConfig.A)(i => {
        val loopActor = ctx.spawnAnonymous(
          Behaviors.setup[Msg.type](ctx => new ThroughputActor(ThroughputConfig.N, latch, ctx)))
        loopActor
      })


      var m = 0
      while (m < ThroughputConfig.N) {

        actors.foreach(loopActor => {
          loopActor ! Msg
        })

        m += 1
      }
    }

    def process(msg: Msg.type) = ()
  }

  private class ThroughputActor(totalMessages: Int, latch: CountDownLatch, ctx: ActorContext[Msg.type])
    extends GCActor[Msg.type](ctx) {

    private var messagesProcessed = 0

    override def process(msg: Msg.type) {

      messagesProcessed += 1
      ThroughputConfig.performComputation(37.2)

      if (messagesProcessed == totalMessages) {
        latch.countDown()
        exit()
      }
    }
  }

}
