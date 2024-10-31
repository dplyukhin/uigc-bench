package edu.rice.habanero.benchmarks.threadring

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.threadring.ThreadRingConfig.{DataMessage, ExitMessage, PingMessage}
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

    private var system: ActorSystem = _
    def runIteration() {

      system = AkkaActorState.newActorSystem("ThreadRing")
      val latch = new CountDownLatch(1)

      val numActorsInRing = ThreadRingConfig.N
      val ringActors = Array.tabulate[ActorRef](numActorsInRing)(i => {
        val loopActor = system.actorOf(Props(new ThreadRingActor(i, numActorsInRing, latch)))
        loopActor
      })

      for ((loopActor, i) <- ringActors.view.zipWithIndex) {
        val nextActor = ringActors((i + 1) % numActorsInRing)
        loopActor ! new DataMessage(nextActor)
      }

      ringActors(0) ! new PingMessage(ThreadRingConfig.R)

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ThreadRingActor(id: Int, numActorsInRing: Int, latch: CountDownLatch) extends AkkaActor[AnyRef] {

    private var nextActor: ActorRef = null

    override def process(msg: AnyRef) {

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

          nextActor = dm.data.asInstanceOf[ActorRef]
      }
    }
  }

}
