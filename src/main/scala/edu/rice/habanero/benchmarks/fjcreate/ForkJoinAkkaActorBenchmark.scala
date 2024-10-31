package edu.rice.habanero.benchmarks.fjcreate

import org.apache.pekko.actor.{ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
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

    private var system: ActorSystem = _
    def runIteration() {

      system = AkkaActorState.newActorSystem("ForkJoin")
      val latch = new CountDownLatch(ForkJoinConfig.N)

      val message = new Object()
      var i = 0
      while (i < ForkJoinConfig.N) {
        val fjRunner = system.actorOf(Props(new ForkJoinActor(latch)))
        fjRunner ! message
        i += 1
      }

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ForkJoinActor(latch: CountDownLatch) extends AkkaActor[AnyRef] {
    override def process(msg: AnyRef) {
      ForkJoinConfig.performComputation(37.2)
      latch.countDown()
      exit()
    }
  }

}
