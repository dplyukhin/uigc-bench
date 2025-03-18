package common

import org.apache.pekko.actor.typed._
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import org.apache.pekko.uigc.actor.typed.RemoteSpawner
import randomworkers.{Protocol, SpawnPoint}

import java.io.{BufferedWriter, FileWriter}
import java.util.concurrent.CountDownLatch
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ClusterBenchmark {

  def main(args: Array[String]): Unit = {
    // Each node has a manager that spawns workers and sends them work.
    // Here we create the benchmark, passing in behaviors for the "leader" manager and for the "follower" managers (if they exist.)
    val bench = new ClusterBenchmark(
      SpawnPoint.leader,
      Map(
        // FIXME Add a flag so I don't have to comment this out manually
        "manager1" -> SpawnPoint.follower(),
        "manager2" -> SpawnPoint.follower()
      )
    )
    bench.runBenchmark(args)
  }

  private def dumpMeasurements(iterationTimes: Iterable[Double]): Unit = {
    val filename = System.getProperty("bench.filename")
    if (filename == null) {
      println("ERROR: Missing filename. Dumping measurements to stdout.")
      for (time <- iterationTimes) println(time)
    } else {
      println(s"Writing measurements to $filename")
      val writer = new BufferedWriter(new FileWriter(filename, true))
      for (time <- iterationTimes) writer.write(time + "\n")
      writer.close()
    }
  }
}

class ClusterBenchmark(
    orchestratorBehavior: (
        ActorRef[MetaProtocol],
        Map[String, ActorRef[RemoteSpawner.Command[Protocol]]],
        Boolean
    ) => Behavior[RemoteSpawner.Command[Protocol]],
    workerBehaviors: Map[String, Behavior[RemoteSpawner.Command[Protocol]]]
) {

  import ClusterBenchmark._

  private val numWorkers = workerBehaviors.size
  private val OrchestratorServiceKey = ServiceKey[MetaProtocol]("ClusterBench")

  def runBenchmark(args: Array[String]): Unit =
    if (args.length != 3) {
      println(
        s"Invalid arguments. Expected 3 args: {role} {hostname} {leader hostname}.\nGot ${args
            .mkString("Array(", ", ", ")")}."
      )
      System.exit(1)
    } else {
      val role = args(0)
      val hostname = args(1)
      val leaderhost = args(2)
      if (role == "orchestrator") startup(role, 25251, hostname, leaderhost)
      else startup(role, 0, hostname, leaderhost)
    }

  private def startup(
      role: String,
      port: Int,
      hostname: String = "127.0.0.1",
      leaderhost: String = "127.0.0.1"
  ): Unit = {
    // Override the configuration of the port when specified as program argument
    val config = ConfigFactory
      .parseString(s"""
      pekko.remote.artery.canonical.hostname=$hostname
      pekko.remote.artery.canonical.port=$port
      pekko.cluster.roles = [$role]
      pekko.cluster.seed-nodes = ["pekko://ClusterSystem@$leaderhost:25251"]
      """)
      .withFallback(ConfigFactory.load("cluster"))

    // Load parameters
    val benchConfig = ConfigFactory.load("benchmark")
    val warmupIterations = benchConfig.getInt("bench.warmup-iter")
    val normalIterations = benchConfig.getInt("bench.iterations")
    var iterationTimes = Seq[Double]()

    for (i <- 1 to (warmupIterations + normalIterations)) {

      // Start the system and wait for it to be ready
      val readyLatch = new CountDownLatch(1)
      val doneLatch = new CountDownLatch(1)
      val isWarmup = i <= warmupIterations
      val system =
        if (role == "orchestrator")
          ActorSystem[MetaProtocol](
            Orchestrator(readyLatch, doneLatch, i - warmupIterations, isWarmup),
            "ClusterSystem",
            config
          )
        else
          ActorSystem[MetaProtocol](Worker(role, readyLatch, doneLatch), "ClusterSystem", config)
      readyLatch.await()
      println("Ready!")

      // Iteration is now running
      val startTime = System.nanoTime()
      doneLatch.await()
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
      system.terminate()
      Await.ready(system.whenTerminated, Duration.Inf)
      System.gc()
      Thread.sleep(10000) // Wait for the partner to shutdown too
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

  object Orchestrator {

    def apply(
        readyLatch: CountDownLatch,
        doneLatch: CountDownLatch,
        iteration: Int,
        isWarmup: Boolean
    ): Behavior[MetaProtocol] = Behaviors.setup[MetaProtocol] { ctx =>
      ctx.system.receptionist ! Receptionist.Register(OrchestratorServiceKey, ctx.self)
      if (numWorkers > 0)
        waitForWorkerNodes(workerNodes = Map(), workerActors = Map(), iteration, readyLatch, doneLatch, isWarmup)
      else {
        ctx.spawnAnonymous(orchestratorBehavior(ctx.self, Map(), isWarmup))
        waitForOrchestrator(Map(), iteration, readyLatch, doneLatch, isWarmup)
      }
    }

    /** When the benchmark is first started, the orchestrator node waits for all the worker nodes to
      * join the cluster.
      */
    private def waitForWorkerNodes(
                                    workerNodes: Map[String, ActorRef[MetaProtocol]],
                                    workerActors: Map[String, ActorRef[RemoteSpawner.Command[Protocol]]],
                                    iteration: Int,
                                    readyLatch: CountDownLatch,
                                    doneLatch: CountDownLatch,
                                    isWarmup: Boolean
    ): Behavior[MetaProtocol] =
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case WorkerJoinedMessage(role, node, rootActor) =>
            val newWorkerNodes = workerNodes + (role -> node)
            val newWorkerActors = workerActors + (role -> rootActor)
            if (newWorkerNodes.size < numWorkers)
              waitForWorkerNodes(newWorkerNodes, newWorkerActors, iteration, readyLatch, doneLatch, isWarmup)
            else {
              ctx.spawnAnonymous(orchestratorBehavior(ctx.self, newWorkerActors, isWarmup))
              waitForOrchestrator(newWorkerNodes, iteration, readyLatch, doneLatch, isWarmup)
            }
        }
      }

    /** After the orchestrator node learned the names of the worker actors, it spawned an
      * orchestrator actor. Here the orch node waits for the orch actor to prepare for a new
      * iteration of the benchmark. Once [[OrchestratorReady]] is received, the node starts a timer.
      */
    private def waitForOrchestrator(
                                     workerNodes: Map[String, ActorRef[MetaProtocol]],
                                     iteration: Int,
                                     readyLatch: CountDownLatch,
                                     doneLatch: CountDownLatch,
                                     isWarmup: Boolean
    ): Behavior[MetaProtocol] =
      Behaviors.receive { (_, msg) =>
        msg match {
          case OrchestratorReady() =>
            readyLatch.countDown()
            waitForIterationCompletion(workerNodes, iteration, doneLatch, isWarmup)
        }
      }

    /** The orchestrator waits to receive [[OrchestratorDone]] and decides whether to do another
      * iteration.
      */
    private def waitForIterationCompletion(
                                            workerNodes: Map[String, ActorRef[MetaProtocol]],
                                            iteration: Int,
                                            doneLatch: CountDownLatch,
                                            isWarmup: Boolean
    ): Behavior[MetaProtocol] =
      Behaviors.receive { (_, msg) =>
        msg match {
          case OrchestratorDone(results, filename) =>
            doneLatch.countDown()
            for ((_, worker) <- workerNodes)
              worker ! IterationDone()
            Behaviors.stopped
        }
      }
  }

  private object Worker {
    def apply(
        role: String,
        readyLatch: CountDownLatch,
        doneLatch: CountDownLatch
    ): Behavior[MetaProtocol] = Behaviors.setup[MetaProtocol] { ctx =>
      val adapter = ctx.messageAdapter[Receptionist.Listing](ReceptionistListing.apply)
      ctx.system.receptionist ! Receptionist.Subscribe(OrchestratorServiceKey, adapter)
      val rootActor = ctx.spawnAnonymous(workerBehaviors(role))
      readyLatch.countDown()
      waitForOrchestrator(role, rootActor, doneLatch)
    }

    /** Worker node waits to receive a reference to the orchestrator node. */
    private def waitForOrchestrator(
        role: String,
        rootActor: ActorRef[RemoteSpawner.Command[Protocol]],
        doneLatch: CountDownLatch
    ): Behavior[MetaProtocol] =
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case ReceptionistListing(OrchestratorServiceKey.Listing(listings)) =>
            listings.find(_ => true) match {
              case Some(orchestratorNode) =>
                orchestratorNode ! WorkerJoinedMessage(role, ctx.self, rootActor)
                prepareForTermination(doneLatch)
              case None =>
                waitForOrchestrator(role, rootActor, doneLatch)
            }
        }
      }

    private def prepareForTermination(
        doneLatch: CountDownLatch
    ): Behavior[MetaProtocol] =
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case IterationDone() =>
            doneLatch.countDown()
            Behaviors.stopped
        }
      }
  }
}
