package edu.rice.habanero.benchmarks.chameneos

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

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

    private var system: ActorSystem = _
    def runIteration() {

      system = AkkaActorState.newActorSystem("Chameneos")

      val latch = new CountDownLatch(1)
      val mallActor = system.actorOf(Props(
        new ChameneosMallActor(
          ChameneosConfig.numMeetings, ChameneosConfig.numChameneos, latch)))

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ChameneosMallActor(var n: Int, numChameneos: Int, latch: CountDownLatch) extends AkkaActor[ChameneosHelper.Message] {

    private var waitingChameneo: ActorRef = null
    private var sumMeetings: Int = 0
    private var numFaded: Int = 0

    startChameneos()

    private def startChameneos() {
      Array.tabulate[ActorRef](numChameneos)(i => {
        val color = ChameneosHelper.Color.values()(i % 3)
        val loopChamenos = context.actorOf(Props(new ChameneosChameneoActor(self, color, i)))
        loopChamenos
      })
    }

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetingCountMsg =>
          numFaded = numFaded + 1
          sumMeetings = sumMeetings + message.count
          if (numFaded == numChameneos) {
            latch.countDown()
          }
        case message: ChameneosHelper.MeetMsg =>
          if (n > 0) {
            if (waitingChameneo == null) {
              val sender = message.sender.asInstanceOf[ActorRef]
              waitingChameneo = sender
            }
            else {
              n = n - 1
              waitingChameneo ! msg
              waitingChameneo = null
            }
          }
          else {
            val sender = message.sender.asInstanceOf[ActorRef]
            sender ! new ChameneosHelper.ExitMsg(self)
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class ChameneosChameneoActor(mall: ActorRef, var color: ChameneosHelper.Color, id: Int)
    extends AkkaActor[ChameneosHelper.Message] {

    private var meetings: Int = 0

    mall ! new ChameneosHelper.MeetMsg(color, self)

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetMsg =>
          val otherColor: ChameneosHelper.Color = message.color
          val sender = message.sender.asInstanceOf[ActorRef]
          color = ChameneosHelper.complement(color, otherColor)
          meetings = meetings + 1
          sender ! new ChameneosHelper.ChangeMsg(color, self)
          mall ! new ChameneosHelper.MeetMsg(color, self)
        case message: ChameneosHelper.ChangeMsg =>
          color = message.color
          meetings = meetings + 1
          mall ! new ChameneosHelper.MeetMsg(color, self)
        case message: ChameneosHelper.ExitMsg =>
          val sender = message.sender.asInstanceOf[ActorRef]
          color = ChameneosHelper.fadedColor
          sender ! new ChameneosHelper.MeetingCountMsg(meetings, self)
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
