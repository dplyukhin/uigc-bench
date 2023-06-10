import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import common.ClusterBenchmark

import scala.concurrent.duration.DurationInt

object ThroughputBenchmark {

  trait Protocol
  private case class SetOrchestrator(orchestrator: ActorRef[Protocol]) extends Protocol
  private case object NewRequest extends Protocol

  object Orchestrator {
    def apply(
               parent: ActorRef[ClusterBenchmark.Protocol[Protocol]],
               workers: Map[String, ActorRef[Protocol]],
               isWarmup: Boolean
             ): Behavior[Protocol] =
      Behaviors.setup { ctx =>
        val config = ConfigFactory.load("throughput-bench")
        val REQS_PER_SECOND = config.getInt("throughput-bench.reqs-per-second")

        val worker1 = workers("worker1")
        val self = ctx.self

        parent ! ClusterBenchmark.OrchestratorReady()
        worker1 ! SetOrchestrator(self)

        Behaviors
          .withTimers[Protocol] { timers =>
            timers.startTimerAtFixedRate((), NewRequest, (1000000000 / REQS_PER_SECOND).nanos)
            Behaviors.empty
          }
      }
  }

  def main(args: Array[String]): Unit =
    ClusterBenchmark(
      Orchestrator.apply,
      Map(
        "worker1" -> Behaviors.ignore[Protocol],
      )
    ).runBenchmark(args)
}