package edu.rice.habanero.benchmarks.randomgraphs

import akka.actor.typed.{Behavior => AkkaBehavior}
import edu.rice.habanero.actors.AkkaActorState
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import gc._



object RandomGraphsAkkaGCActorBenchmark {

  def main(args: Array[String]): Unit = {
    BenchmarkRunner.runBenchmark(args, new RandomGraphsAkkaGCActorBenchmark)
  }


  sealed trait Msg extends Message

  final case class Link(ref: ActorRef[Msg]) extends Msg {
    def refs = Seq(ref)
  }

  final case class Ping() extends Msg {
    def refs = Seq()
  }

  private final class RandomGraphsAkkaGCActorBenchmark extends Benchmark {
    def initialize(args: Array[String]): Unit = {
      AkkaActorState.initialize()
    }

    def printArgInfo(): Unit = {
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      Thread.sleep(5000) // Give the actor system time to shut down
    }


    def runIteration(): Unit = {
      val stats = new Statistics

      val system = AkkaActorState.newActorSystem("RandomGraphs", BenchmarkActor.createRoot(stats))

      for (_ <- 1 to RandomGraphsParam.NumberOfPingsSent) {
        system ! Ping()
      }
      try {
        stats.latch.await()
        system.terminate()
        println(stats)
      } catch {
        case ex: InterruptedException =>
          ex.printStackTrace()
      }
    }

  }

  object BenchmarkActor {
    def apply(statistics: Statistics): ActorFactory[Msg] = {
      Behaviors.setup(context => new BenchmarkActor(context, statistics))
    }

    def createRoot(statistics: Statistics): AkkaBehavior[Msg] = {
      Behaviors.setupReceptionist(context => new BenchmarkActor(context, statistics))
    }
  }

  private class BenchmarkActor(context: ActorContext[Msg], stats: Statistics)
    extends AbstractBehavior[Msg](context) with RandomGraphsActor[ActorRef[Msg]] {


    override val statistics: Statistics = stats
    override val debug: Boolean = true

    override def spawn(): ActorRef[Msg] =
      context.spawnAnonymous(BenchmarkActor(stats))

    override def linkActors(owner: ActorRef[Msg], target: ActorRef[Msg]): Unit = {
      val ref = context.createRef(target, owner)
      owner ! Link(ref)
      super.linkActors(owner, target)
    }

    override def forgetActor(ref: ActorRef[Msg]): Unit = {
      context.release(ref)
      super.forgetActor(ref)
    }

    override def ping(ref: ActorRef[Msg]): Unit = {
      ref ! Ping()
      super.ping(ref)
    }

    override def onMessage(msg: Msg): Behavior[Msg] = {

      msg match {
        case Link(ref) =>
          acquaintances += ref
          doSomeActions()
          this

        case Ping() =>
          doSomeActions()
          this
      }
    }


  }
}
