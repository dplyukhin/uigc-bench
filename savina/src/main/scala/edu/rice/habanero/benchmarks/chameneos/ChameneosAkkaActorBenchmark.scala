package edu.rice.habanero.benchmarks.chameneos

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
object ChameneosAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ChameneosAkkaActorBenchmark)
  }

  private final class ChameneosAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ChameneosConfig.parseArgs(args)
    }

    def printArgInfo() {
      ChameneosConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot[Msg](ctx =>
          new ChameneosMallActor(ChameneosConfig.numMeetings, ChameneosConfig.numChameneos, latch, ctx)),
        "Chameneos")

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }


  protected trait Msg extends Message
  case class MallMsg (val mall: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(mall)
  }
  case class MeetMsg (val color: ChameneosHelper.Color, val sender: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }
  case class ChangeMsg (val color: ChameneosHelper.Color) extends Msg with NoRefs
  case class MeetingCountMsg (val count: Int) extends Msg with NoRefs
  case object ExitMsg extends Msg with NoRefs

  private class ChameneosMallActor(var n: Int, numChameneos: Int, latch: CountDownLatch, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {

    private var waitingChameneo: ActorRef[Msg] = null
    private var sumMeetings: Int = 0
    private var numFaded: Int = 0

    startChameneos()

    private def startChameneos() {
      Array.tabulate[ActorRef[Msg]](numChameneos)(i => {
        val color = ChameneosHelper.Color.values()(i % 3)
        val loopChamenos: ActorRef[Msg] = ctx.spawnAnonymous(Behaviors.setup[Msg]{ ctx =>
          new ChameneosChameneoActor(color, i, ctx)
        })
        loopChamenos ! MallMsg(ctx.createRef(ctx.self, loopChamenos))
        loopChamenos
      })
    }

    override def process(msg: Msg) {
      msg match {
        case MeetingCountMsg(count) =>
          numFaded = numFaded + 1
          sumMeetings = sumMeetings + count
          if (numFaded == numChameneos) {
            latch.countDown()
          }
        case MeetMsg(color, sender) =>
          if (n > 0) {
            if (waitingChameneo == null) {
              waitingChameneo = sender
            }
            else {
              n = n - 1
              waitingChameneo ! MeetMsg(color, ctx.createRef(sender, waitingChameneo))
              waitingChameneo = null
            }
          }
          else {
            sender ! ExitMsg
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class ChameneosChameneoActor(var color: ChameneosHelper.Color, id: Int, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {

    private var mall: ActorRef[Msg] = _
    private var meetings: Int = 0

    override def process(msg: Msg) {
      msg match {
        case MallMsg(mall) =>
          this.mall = mall
          mall ! MeetMsg(color, ctx.createRef(ctx.self, mall))
        case MeetMsg(otherColor, sender) =>
          color = ChameneosHelper.complement(color, otherColor)
          meetings = meetings + 1
          sender ! ChangeMsg(color)
          mall ! MeetMsg(color, ctx.createRef(ctx.self, mall))
        case ChangeMsg(color) =>
          meetings = meetings + 1
          mall ! MeetMsg(color, ctx.createRef(ctx.self, mall))
        case ExitMsg =>
          color = ChameneosHelper.fadedColor
          mall ! MeetingCountMsg(meetings)
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
