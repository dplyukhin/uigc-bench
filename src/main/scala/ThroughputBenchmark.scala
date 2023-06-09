package cluster.putDatum

import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import common.ClusterBenchmark

object ThroughputBenchmark {

  def main(args: Array[String]): Unit =
    ClusterBenchmark(
      Orchestrator.apply,
      Map(
        "worker1" -> Behaviors.empty,
        "worker2" -> Behaviors.empty
      )
    ).runBenchmark(args)
}