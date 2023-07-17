import akka.actor.typed._
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import com.typesafe.config.ConfigFactory
import common.ClusterBenchmark
import common.CborSerializable

import scala.concurrent.duration.DurationInt

object ThroughputBenchmark {

  trait Protocol extends CborSerializable
  private case class SetOrchestrator(orchestrator: ActorRef[Protocol]) extends Protocol
  private case object NewRequest extends Protocol
  private case object Ping extends Protocol
  private case object Ack extends Protocol

  object Orchestrator {
    def apply(
               parent: ActorRef[ClusterBenchmark.Protocol[Protocol]],
               workers: Map[String, ActorRef[Protocol]],
               isWarmup: Boolean
             ): Behavior[Protocol] =
      Behaviors.setup { ctx =>
        val config = ConfigFactory.load("throughput-bench")
        val REQS_PER_SECOND = config.getInt("throughput-bench.reqs-per-second")
        val ITERATIONS = config.getInt("throughput-bench.iterations")

        val worker = workers("worker")
        val self = ctx.self

        parent ! ClusterBenchmark.OrchestratorReady()
        worker ! SetOrchestrator(self)

        Behaviors
          .withTimers[Protocol] { timers =>
            timers.startTimerAtFixedRate((), NewRequest, (1000000000 / REQS_PER_SECOND).nanos)
            loop(parent, worker, timers, ITERATIONS)
          }
      }

    private def loop(
              parent: ActorRef[ClusterBenchmark.Protocol[Protocol]],
              worker: ActorRef[Protocol],
              timers: TimerScheduler[Protocol],
              iterationsLeft: Int
            ): Behavior[Protocol] =
      Behaviors.receiveMessage {
        case NewRequest =>
          worker ! Ping
          if (iterationsLeft <= 0) {
            timers.cancelAll()
            awaitResponse(parent)
          }
          else {
            loop(parent, worker, timers, iterationsLeft - 1)
          }
      }

    private def awaitResponse(
                               parent: ActorRef[ClusterBenchmark.Protocol[Protocol]],
                             ): Behavior[Protocol] =
      Behaviors.receiveMessage {
        case Ack =>
          parent ! ClusterBenchmark.OrchestratorDone()
          Behaviors.stopped
      }
  }

  object Worker {
    def apply(): Behavior[Protocol] =
      Behaviors.receiveMessage {
        case SetOrchestrator(orch) =>
          val config = ConfigFactory.load("throughput-bench")
          val ITERATIONS = config.getInt("throughput-bench.iterations")
          loop(orch, ITERATIONS)
      }

    private def loop(
                      orch: ActorRef[Protocol],
                      iterationsLeft: Int
                    ): Behavior[Protocol] =
      Behaviors.receiveMessage {
        case Ping =>
          if (iterationsLeft <= 0) {
            orch ! Ack
            Behaviors.stopped
          }
          else {
            loop(orch, iterationsLeft - 1)
          }
      }
  }

  def main(args: Array[String]): Unit =
    ClusterBenchmark(
      Orchestrator.apply,
      Map(
        "worker" -> Worker(),
      )
    ).runBenchmark(args)
}