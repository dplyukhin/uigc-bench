package edu.rice.habanero.benchmarks.cigsmok

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch

/**
 * Based on solution in <a href="http://en.wikipedia.org/wiki/Cigarette_smokers_problem">Wikipedia</a> where resources are acquired instantaneously.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CigaretteSmokerAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CigaretteSmokerAkkaActorBenchmark)
  }

  private final class CigaretteSmokerAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CigaretteSmokerConfig.parseArgs(args)
    }

    def printArgInfo() {
      CigaretteSmokerConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new ArbiterActor(CigaretteSmokerConfig.R, CigaretteSmokerConfig.S, latch, ctx)
        ),
        "CigaretteSmoker")
      system ! StartMessage
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  private case class StartSmoking(val busyWaitPeriod: Int) extends Msg with NoRefs
  private case object StartedSmoking extends Msg with NoRefs
  private case object StartMessage extends Msg with NoRefs
  private case object ExitMessage extends Msg with NoRefs

  private class ArbiterActor(numRounds: Int, numSmokers: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val smokerActors = Array.tabulate[ActorRef[Msg]](numSmokers)(i => {
      val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SmokerActor(ctx) })
      actor ! Rfmsg(ctx.createRef(ctx.self, actor))
      actor
    })
    private val random = new PseudoRandom(numRounds * numSmokers)
    private var roundsSoFar = 0

    override def process(msg: Msg) {
      msg match {
        case StartMessage =>

          // choose a random smoker to start smoking
          notifyRandomSmoker()

        case StartedSmoking =>

          // resources are off the table, can place new ones on the table
          roundsSoFar += 1
          if (roundsSoFar >= numRounds) {
            latch.countDown()
          } else {
            // choose a random smoker to start smoking
            notifyRandomSmoker()
          }
      }
    }

    private def notifyRandomSmoker() {
      // assume resources grabbed instantaneously
      val newSmokerIndex = Math.abs(random.nextInt()) % numSmokers
      val busyWaitPeriod = random.nextInt(1000) + 10
      smokerActors(newSmokerIndex) ! new StartSmoking(busyWaitPeriod)
    }

    private def requestSmokersToExit() {
      smokerActors.foreach(loopActor => {
        loopActor ! ExitMessage
      })
    }
  }

  private class SmokerActor(ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    var arbiterActor: ActorRef[Msg] = _
    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) => this.arbiterActor = x
        case sm: StartSmoking =>

          // notify arbiter that started smoking
          arbiterActor ! StartedSmoking
          // now smoke cigarette
          CigaretteSmokerConfig.busyWait(sm.busyWaitPeriod)

        case ExitMessage =>

          exit()
      }
    }
  }

}
