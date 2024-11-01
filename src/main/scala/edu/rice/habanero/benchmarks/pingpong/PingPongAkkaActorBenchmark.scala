package edu.rice.habanero.benchmarks.pingpong

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
object PingPongAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PingPongAkkaActorBenchmark)
  }

  private final class PingPongAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PingPongConfig.parseArgs(args)
    }

    def printArgInfo() {
      PingPongConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot[Msg](ctx =>
          new Master(latch, ctx)),
        "PingPong")

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  case class StartMessage(pong: ActorRef[Msg]) extends Msg with NoRefs
  case object StopMessage extends Msg with NoRefs
  case object PingMessage extends Msg with NoRefs
  case object SendPongMessage extends Msg with NoRefs
  case class SendPingMessage(sender: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }

  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {
      val pong = ctx.spawn(Behaviors.setup[Msg](ctx => new PongActor(ctx)), "Pong")
      val ping = ctx.spawn(Behaviors.setup[Msg](ctx => new PingActor(PingPongConfig.N, latch, ctx)), "Ping")
      ping ! StartMessage(pong)
    }

    def process(msg: Msg) = ()
  }

  private class PingActor(count: Int, latch: CountDownLatch, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {

    private var pong: ActorRef[Msg] = _
    private var pingsLeft: Int = count

    override def process(msg: Msg) {
      msg match {
        case StartMessage(pong) =>
          this.pong = pong
          pong ! SendPingMessage(ctx.createRef(ctx.self, pong))
          pingsLeft = pingsLeft - 1
        case PingMessage =>
          pong ! SendPingMessage(ctx.createRef(ctx.self, pong))
          pingsLeft = pingsLeft - 1
        case SendPongMessage =>
          if (pingsLeft > 0) {
            ctx.self ! PingMessage
          } else {
            latch.countDown()
            pong ! StopMessage
            exit()
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class PongActor(ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    private var pongCount: Int = 0

    override def process(msg: Msg) {
      msg match {
        case SendPingMessage(sender) =>
          sender ! SendPongMessage
          pongCount = pongCount + 1
        case StopMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
