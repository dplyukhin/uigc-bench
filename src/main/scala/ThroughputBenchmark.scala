package cluster.putDatum

import akka.actor.typed.*
import akka.actor.typed.scaladsl.Behaviors
import cluster.SimpleBenchmark

object ThroughputBenchmark {

  def main(args: Array[String]): Unit =
    SimpleBenchmark(
      Orchestrator.apply,
      Map(
        "worker1" -> Behaviors.empty,
        "worker2" -> Behaviors.empty
      )
    ).runBenchmark(args)
}