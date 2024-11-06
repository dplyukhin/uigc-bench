package edu.rice.habanero.benchmarks.big

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{GCActor, AkkaActorState}
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

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(BigConfig.W)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot[Msg](ctx => new Master(latch, ctx)), "Big")

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  case class NeighborMessage(neighbors: Array[ActorRef[Msg]]) extends Msg {
    def refs: Iterable[ActorRef[_]] = neighbors
  }
  final class PingMessage(val sender: Int) extends Msg with NoRefs
  final class PongMessage(val sender: Int) extends Msg with NoRefs
  final object ExitMessage extends Msg with NoRefs

  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {
      val sinkActor = ctx.spawnAnonymous(Behaviors.setup[Msg](ctx => new SinkActor(BigConfig.W, ctx)))

      val bigActors = Array.tabulate[ActorRef[Msg]](BigConfig.W)(i => {
        val loopActor = ctx.spawnAnonymous(Behaviors.setup[Msg](ctx => new BigActor(i, BigConfig.N, latch, ctx)))
        loopActor ! Rfmsg(ctx.createRef(sinkActor, loopActor))
        loopActor
      })

      sinkActor ! NeighborMessage(
        bigActors.map(target => ctx.createRef(target, sinkActor))
      )
      bigActors.foreach(loopActor => {
        loopActor ! NeighborMessage(
          bigActors.map(target => ctx.createRef(target, loopActor))
        )
      })

      bigActors.foreach(loopActor => {
        loopActor ! new PongMessage(-1)
      })
    }

    def process(msg: Msg): Unit = ()
  }

  private class BigActor(id: Int, numMessages: Int, latch: CountDownLatch, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {
    var sinkActor: ActorRef[Msg] = _
    private var numPings = 0
    private var expPinger = -1
    private val random = new PseudoRandom(id)
    private var neighbors: Array[ActorRef[Msg]] = null

    private val myPingMessage = new PingMessage(id)
    private val myPongMessage = new PongMessage(id)

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) => this.sinkActor = x
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

  private class SinkActor(numWorkers: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var numMessages = 0
    private var neighbors: Array[ActorRef[Msg]] = null

    override def process(msg: Msg) {
      msg match {
        case nm: NeighborMessage =>

          neighbors = nm.neighbors
      }
    }
  }

}
