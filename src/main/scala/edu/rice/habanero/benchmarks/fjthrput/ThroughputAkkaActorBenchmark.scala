package edu.rice.habanero.benchmarks.fjthrput

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

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

    private var system: ActorSystem = _
    def runIteration() {

      system = AkkaActorState.newActorSystem("Throughput")
      val latch = new CountDownLatch(ThroughputConfig.A)

      val actors = Array.tabulate[ActorRef](ThroughputConfig.A)(i => {
        val loopActor = system.actorOf(Props(new ThroughputActor(ThroughputConfig.N, latch)))
        loopActor
      })

      val message = new Object()

      var m = 0
      while (m < ThroughputConfig.N) {

        actors.foreach(loopActor => {
          loopActor ! message
        })

        m += 1
      }

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ThroughputActor(totalMessages: Int, latch: CountDownLatch) extends AkkaActor[AnyRef] {

    private var messagesProcessed = 0

    override def process(msg: AnyRef) {

      messagesProcessed += 1
      ThroughputConfig.performComputation(37.2)

      if (messagesProcessed == totalMessages) {
        latch.countDown()
        exit()
      }
    }
  }

}
