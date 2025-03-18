package meta

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed._

import java.io.{BufferedWriter, FileWriter}
import java.util.concurrent.CountDownLatch
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object BenchmarkRunner {

  def main(args: Array[String]): Unit =
    if (args.length != 4) {
      println(
        s"Invalid arguments. Expected 4 args: {number of nodes} {role} {hostname} {leader hostname}.\nGot ${args
            .mkString("Array(", ", ", ")")}."
      )
      System.exit(1)
    } else {
      val numNodes   = args(0).toInt
      val role       = args(1)
      val hostname   = args(2)
      val leaderhost = args(3)

      if (role == "orchestrator") startup(role, 25251, hostname, leaderhost, numNodes)
      else startup(role, 0, hostname, leaderhost, numNodes)
    }

  private def startup(
      role: String,
      port: Int,
      hostname: String,
      leaderhost: String,
      numNodes: Int
  ): Unit = {
    // Override the configuration of the port when specified as program argument
    val config: Config = ConfigFactory
      .parseString(s"""
      pekko.remote.artery.canonical.hostname=$hostname
      pekko.remote.artery.canonical.port=$port
      pekko.cluster.roles = [$role]
      pekko.cluster.seed-nodes = ["pekko://ClusterSystem@$leaderhost:25251"]
      """)
      .withFallback(ConfigFactory.load("cluster"))
      .withFallback(ConfigFactory.load("benchmark"))
      .withFallback(ConfigFactory.load("random-workers"))

    var iterationTimes   = Seq[Double]()
    val warmupIterations = config.getInt("bench.warmup-iter")
    val normalIterations = config.getInt("bench.iterations")

    for (i <- 1 to (warmupIterations + normalIterations)) {

      // Start the system and wait for it to be ready
      println(s"Setting up iteration $i...")
      val info = new IterationInfo(
        role = role,
        readyLatch = new CountDownLatch(1),
        doneLatch = new CountDownLatch(1),
        numNodes = numNodes,
        iteration = i,
        isWarmup = i <= warmupIterations
      )
      val system =
        if (role == "orchestrator")
          ActorSystem[MetaProtocol](Orchestrator(info), "ClusterSystem", config)
        else
          ActorSystem[MetaProtocol](Worker(info), "ClusterSystem", config)
      info.readyLatch.await()
      println(s"Starting iteration $i...")

      // Iteration is now running
      val startTime = System.nanoTime()
      info.doneLatch.await()
      val endTime = System.nanoTime()

      // Orchestrator logs iteration times
      if (role == "orchestrator") {
        val execTimeMillis = (endTime - startTime) / 1e6
        if (i <= warmupIterations) {
          println(s"Warmup iteration $i: $execTimeMillis ms")
        } else {
          val j = i - warmupIterations
          iterationTimes = iterationTimes :+ execTimeMillis
          println(s"Iteration ${j}: $execTimeMillis ms")
        }
      }

      // Clean up the system for another iteration
      println(s"Tearing down actor system...")
      system.terminate()
      Await.ready(system.whenTerminated, Duration.Inf)
      System.gc()
      Thread.sleep(10000) // Wait for the other systems to shutdown too
    }

    // All iterations are done
    if (role == "orchestrator") {
      val avg = iterationTimes.sum / iterationTimes.length
      val min = iterationTimes.min
      val max = iterationTimes.max
      println(s"\nAverage: ${avg}\nMinimum: ${min}\nMaximum: ${max}")
      dumpMeasurements(iterationTimes)
    }
  }

  private def dumpMeasurements(iterationTimes: Iterable[Double]): Unit = {
    val filename = System.getProperty("bench.filename")
    if (filename == null) {
      println("Missing filename. Dumping measurements to stdout.")
      for (time <- iterationTimes) println(time)
    } else {
      println(s"Writing measurements to $filename")
      val writer = new BufferedWriter(new FileWriter(filename, true))
      for (time <- iterationTimes) writer.write(time + "\n")
      writer.close()
    }
  }

}
