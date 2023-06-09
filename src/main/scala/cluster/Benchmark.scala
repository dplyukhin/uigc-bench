package cluster
import akka.actor.typed._
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory

import java.io.{BufferedWriter, FileWriter}

/** Cluster benchmarks follow this protocol.
  *
  * Part 1: Initialization.
  *   1. Start up a system for the orchestrator and one for each worker. 2. The orchestrator system
  *      publishes the name of its root actor. 3. Every worker system subscribes to the orchestrator
  *      service key and waits for the orchestrator name.
  *
  * Part 2: Prepare for the iteration.
  *   1. The orchestrator system asks every worker system to spawn its worker. 2. Every worker
  *      system does so, and replies with a reference to its worker. 3. The orchestrator waits until
  *      it has names for every worker in this iteration.
  *
  * Part 3: Run the iteration.
  *   1. The orchestrator system starts the timer and spawns the orchestrator actor with references
  *      to the workers. 2. The orchestrator runs until the protocol is done, then notifies its
  *      parent to stop and terminates itself. 3. Workers should terminate themselves when they are
  *      done with the protocol.
  *
  * Part 4: Cleanup.
  *   1. The orchestrator system stops the timer. 2. If there are more iterations, proceed back to
  *      Part 2.
  */

object Benchmark {
  def dumpMeasurements(iterationTimes: Iterable[Double], filename: String): Unit =
    if (filename == null) {
      println("ERROR: Missing filename. Dumping measurements to stdout.")
      for (time <- iterationTimes) println(time)
    } else {
      println(s"Writing measurements to $filename")
      val writer = new BufferedWriter(new FileWriter(filename, true))
      for (time <- iterationTimes) writer.write(time + "\n")
      writer.close()
    }

  /** A benchmark with only one actor per system */
  def SimpleBenchmark(
      orchestratorBehavior: (
          ActorRef[Benchmark.Protocol],
          Map[String, ActorRef[Nothing]],
          Boolean
      ) => Behavior[Nothing],
      workerBehaviors: Map[String, Behavior[Nothing]]
  ): Benchmark = {
    val workerBehaviors2 =
      for ((name, behavior) <- workerBehaviors) yield name -> Map(name -> behavior)
    def orchestratorBehavior2(
        parentRef: ActorRef[Benchmark.Protocol],
        workerNodes: Map[String, Map[String, ActorRef[Nothing]]],
        isWarmup: Boolean
    ): Behavior[Nothing] = {
      val workerRefs =
        for ((_, map) <- workerNodes; (name, ref) <- map) yield name -> ref
      orchestratorBehavior(parentRef, workerRefs, isWarmup)
    }
    new Benchmark(
      orchestratorBehavior2,
      workerBehaviors2
    )
  }

  trait Protocol
  case class WorkerJoinedMessage(role: String, ref: ActorRef[Protocol]) extends Protocol
  case class SpawnWorkerAck(role: String, ref: Map[String, ActorRef[Nothing]]) extends Protocol
  case class ReceptionistListing(listing: Receptionist.Listing) extends Protocol
  case object SpawnWorker extends Protocol
  case object OrchestratorReady extends Protocol
  case object OrchestratorDone extends Protocol
}

class Benchmark(
    orchestratorBehavior: (
        ActorRef[Benchmark.Protocol],
        Map[String, Map[String, ActorRef[Nothing]]],
        Boolean
    ) => Behavior[Nothing],
    workerBehaviors: Map[String, Map[String, Behavior[Nothing]]]
) {

  import Benchmark._

  private val numWorkers = workerBehaviors.size
  private val OrchestratorServiceKey = ServiceKey[Protocol]("ClusterBench")

  def runBenchmark(args: Array[String]): Unit =
    // Run them all on the same node
    if (args.isEmpty) {
      startup("orchestrator", 25251)
      for ((name, _) <- workerBehaviors) startup(name, 0)
    } else if (args.length != 3) {
      println(
        s"Invalid arguments. Expected 3 args: {role} {hostname} {leader hostname}.\nGot ${args.mkString("Array(", ", ", ")")}."
      )
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
      akka.remote.artery.canonical.hostname=$hostname
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      akka.cluster.seed-nodes = ["akka://ClusterSystem@$leaderhost:25251"]
      """)
      .withFallback(ConfigFactory.load("application"))

    if (role == "orchestrator")
      ActorSystem[Protocol](Orchestrator(), "ClusterSystem", config)
    else ActorSystem[Protocol](Worker(role), "ClusterSystem", config)
  }

  object Orchestrator {

    def apply(): Behavior[Protocol] = Behaviors.setup[Protocol] { ctx =>
      ctx.system.receptionist ! Receptionist.Register(OrchestratorServiceKey, ctx.self)
      waitForWorkerNodes(workerNodes = Map())
    }

    private def waitForWorkerNodes(
        workerNodes: Map[String, ActorRef[Protocol]]
    ): Behavior[Protocol] =
      Behaviors.receive { (_, msg) =>
        msg match {
          case WorkerJoinedMessage(role, workerNode) =>
            val newWorkerNodes = workerNodes + (role -> workerNode)
            if (newWorkerNodes.size < numWorkers)
              waitForWorkerNodes(newWorkerNodes)
            else {
              for ((_, workerNode) <- newWorkerNodes)
                workerNode ! SpawnWorker
              waitForWorkers(Seq[Double](), newWorkerNodes, Map())
            }
        }
      }

    private def waitForWorkers(
        iterationTimes: Seq[Double],
        workerNodes: Map[String, ActorRef[Protocol]],
        workers: Map[String, Map[String, ActorRef[Nothing]]]
    ): Behavior[Protocol] =
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case SpawnWorkerAck(role, subworkers) =>
            val newWorkers = workers + (role -> subworkers)
            if (newWorkers.size < numWorkers) {
              waitForWorkers(iterationTimes, workerNodes, newWorkers)
            } else {
              val config = ConfigFactory.load("benchmark")
              val warmupIterations = config.getInt("bench.warmup-iter")
              val isWarmup = iterationTimes.length < warmupIterations
              ctx.spawnAnonymous(orchestratorBehavior(ctx.self, newWorkers, isWarmup))
              waitForOrchestrator(iterationTimes, workerNodes)
            }
        }
      }

    private def waitForOrchestrator(
        iterationTimes: Seq[Double],
        workerNodes: Map[String, ActorRef[Protocol]]
    ): Behavior[Protocol] =
      Behaviors.receive { (_, msg) =>
        msg match {
          case OrchestratorReady =>
            val startTime = System.nanoTime()
            waitForIterationCompletion(startTime, iterationTimes, workerNodes)
        }
      }

    private def waitForIterationCompletion(
        startTime: Double,
        iterationTimes: Seq[Double],
        workerNodes: Map[String, ActorRef[Protocol]]
    ): Behavior[Protocol] =
      Behaviors.receive { (_, msg) =>
        msg match {
          case OrchestratorDone =>
            val endTime = System.nanoTime()
            val iterationTime = (endTime - startTime) / 1e6
            val newIterationTimes = iterationTimes :+ iterationTime

            val config = ConfigFactory.load("benchmark")
            val warmupIterations = config.getInt("bench.warmup-iter")
            val normalIterations = config.getInt("bench.iterations")
            val totalIterations = warmupIterations + normalIterations

            if (newIterationTimes.length <= warmupIterations) {
              println(s"Warmup iteration ${newIterationTimes.length}: $iterationTime ms")
            } else {
              println(
                s"Iteration ${newIterationTimes.length - warmupIterations}: $iterationTime ms"
              )
            }
            if (newIterationTimes.length < totalIterations) {
              for ((_, workerNode) <- workerNodes)
                workerNode ! SpawnWorker
              waitForWorkers(newIterationTimes, workerNodes, Map())
            } else {
              val iterationTimes = newIterationTimes.drop(10)
              val avg = iterationTimes.sum / iterationTimes.length
              val min = iterationTimes.min
              val max = iterationTimes.max
              println(s"\nAverage: $avg\nMinimum: $min\nMaximum: $max")
              dumpMeasurements(iterationTimes, System.getProperty("bench.filename"))
              Behaviors.stopped
            }
        }
      }
  }

  private object Worker {
    def apply(role: String): Behavior[Protocol] = Behaviors.setup[Protocol] { ctx =>
      val adapter = ctx.messageAdapter[Receptionist.Listing](ReceptionistListing.apply)
      ctx.system.receptionist ! Receptionist.Subscribe(OrchestratorServiceKey, adapter)
      waitForOrchestrator(role)
    }

    private def waitForOrchestrator(role: String): Behavior[Protocol] =
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case ReceptionistListing(OrchestratorServiceKey.Listing(listings)) =>
            listings.find(_ => true) match {
              case Some(orchestratorNode) =>
                orchestratorNode ! WorkerJoinedMessage(role, ctx.self)
                prepareForIteration(role, orchestratorNode)
              case None =>
                waitForOrchestrator(role)
            }
        }
      }

    private def prepareForIteration(
        role: String,
        orchestratorNode: ActorRef[Protocol]
    ): Behavior[Protocol] =
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case SpawnWorker =>
            val workers =
              for ((name, worker) <- workerBehaviors(role)) yield name -> ctx.spawnAnonymous(worker)
            orchestratorNode ! SpawnWorkerAck(role, workers)
            prepareForIteration(role, orchestratorNode)
          case _ =>
            Behaviors.stopped
        }
      }
  }
}
