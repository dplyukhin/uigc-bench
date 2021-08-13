package edu.rice.habanero.benchmarks.randomgraphs

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import edu.rice.habanero.actors.AkkaActorState
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}



object RandomGraphsAkkaActorBenchmark {

  def main(args: Array[String]): Unit = {
    BenchmarkRunner.runBenchmark(args, new RandomGraphsAkkaActorBenchmark)
  }

  sealed trait RandomGraphsMsg

  final case class Link(ref: ActorRef[RandomGraphsMsg]) extends RandomGraphsMsg

  final case class Ping() extends RandomGraphsMsg

  private final class RandomGraphsAkkaActorBenchmark extends Benchmark {
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

      val system = AkkaActorState.newActorSystem("RandomGraphs", BenchmarkActor(stats))

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
    def apply(statistics: Statistics): Behavior[RandomGraphsMsg] = {
      Behaviors.setup(context => new BenchmarkActor(context, statistics))
    }
  }

  private class BenchmarkActor(context: ActorContext[RandomGraphsMsg], stats: Statistics)
    extends AbstractBehavior[RandomGraphsMsg](context)
        with RandomGraphsActor[ActorRef[RandomGraphsMsg]] {

    override val statistics: Statistics = stats
    override val debug: Boolean = true

    override def spawn(): ActorRef[RandomGraphsMsg] =
      context.spawnAnonymous(BenchmarkActor(stats))

    override def linkActors(owner: ActorRef[RandomGraphsMsg], target: ActorRef[RandomGraphsMsg]): Unit = {
      owner ! Link(target)
      super.linkActors(owner, target)
    }

    override def ping(ref: ActorRef[RandomGraphsMsg]): Unit = {
      ref ! Ping()
      super.ping(ref)
    }

    override def onMessage(msg: RandomGraphsMsg): Behavior[RandomGraphsMsg] = {
      msg match {
        case Link(ref) =>
          acquaintances += ref
          doSomeActions()
          Behaviors.same

        case Ping() =>
          doSomeActions()
          Behaviors.same
      }
    }


  }
}
