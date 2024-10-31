package edu.rice.habanero.benchmarks.big

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.big.BigConfig.{ExitMessage, Message, PingMessage, PongMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BigAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BigAkkaActorBenchmark)
  }

  private final class BigAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BigConfig.parseArgs(args)
    }

    def printArgInfo() {
      BigConfig.printArgs()
    }

    private var system: ActorSystem = _
    def runIteration() {

      system = AkkaActorState.newActorSystem("Big")

      val sinkActor = system.actorOf(Props(new SinkActor(BigConfig.W)))

      val latch = new CountDownLatch(BigConfig.W)
      val bigActors = Array.tabulate[ActorRef](BigConfig.W)(i => {
        val loopActor = system.actorOf(Props(new BigActor(i, BigConfig.N, sinkActor, latch)))
        loopActor
      })

      val neighborMessage = new NeighborMessage(bigActors)
      sinkActor ! neighborMessage
      bigActors.foreach(loopActor => {
        loopActor ! neighborMessage
      })

      bigActors.foreach(loopActor => {
        loopActor ! new PongMessage(-1)
      })

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private case class NeighborMessage(neighbors: Array[ActorRef]) extends Message

  private class BigActor(id: Int, numMessages: Int, sinkActor: ActorRef, latch: CountDownLatch) extends AkkaActor[AnyRef] {

    private var numPings = 0
    private var expPinger = -1
    private val random = new PseudoRandom(id)
    private var neighbors: Array[ActorRef] = null

    private val myPingMessage = new PingMessage(id)
    private val myPongMessage = new PongMessage(id)

    override def process(msg: AnyRef) {
      msg match {
        case pm: PingMessage =>

          val sender = neighbors(pm.sender)
          sender ! myPongMessage

        case pm: PongMessage =>

          if (pm.sender != expPinger) {
            println("ERROR: Expected: " + expPinger + ", but received ping from " + pm.sender)
          }
          if (numPings == numMessages) {
            latch.countDown()
          } else {
            sendPing()
            numPings += 1
          }

        case nm: NeighborMessage =>

          neighbors = nm.neighbors
      }
    }

    private def sendPing(): Unit = {
      val target = random.nextInt(neighbors.size)
      val targetActor = neighbors(target)

      expPinger = target
      targetActor ! myPingMessage
    }
  }

  private class SinkActor(numWorkers: Int) extends AkkaActor[AnyRef] {

    private var numMessages = 0
    private var neighbors: Array[ActorRef] = null

    override def process(msg: AnyRef) {
      msg match {
        case nm: NeighborMessage =>

          neighbors = nm.neighbors
      }
    }
  }

}
