package meta

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.uigc.actor.typed.RemoteSpawner
import randomworkers.{Protocol, SpawnPoint}

import java.util.concurrent.CountDownLatch

object Orchestrator {
  val OrchestratorServiceKey: ServiceKey[MetaProtocol] = ServiceKey[MetaProtocol]("ClusterBench")

  def apply(
             info: IterationInfo,
           ): Behavior[MetaProtocol] = Behaviors.setup[MetaProtocol] { ctx =>
    ctx.system.receptionist ! Receptionist.Register(OrchestratorServiceKey, ctx.self)
    if (info.numNodes > 1)
      waitForWorkerNodes(workerNodes = Map(), workerActors = Map(), info)
    else {
      ctx.spawnAnonymous(SpawnPoint.leader(ctx.self, Map(), info.isWarmup))
      waitForOrchestrator(Map(), info)
    }
  }

  /** When the benchmark is first started, the orchestrator node waits for all the worker nodes to
   * join the cluster.
   */
  private def waitForWorkerNodes(
                                  workerNodes: Map[String, ActorRef[MetaProtocol]],
                                  workerActors: Map[String, ActorRef[RemoteSpawner.Command[Protocol]]],
                                  info: IterationInfo,
                                ): Behavior[MetaProtocol] =
    Behaviors.receive { (ctx, msg) =>
      msg match {
        case WorkerJoinedMessage(role, node, rootActor) =>
          val newWorkerNodes = workerNodes + (role -> node)
          val newWorkerActors = workerActors + (role -> rootActor)
          if (newWorkerNodes.size < info.numNodes - 1)
            waitForWorkerNodes(newWorkerNodes, newWorkerActors, info)
          else {
            ctx.spawnAnonymous(SpawnPoint.leader(ctx.self, newWorkerActors, info.isWarmup))
            waitForOrchestrator(newWorkerNodes, info)
          }
      }
    }

  /** After the orchestrator node learned the names of the worker actors, it spawned an
   * orchestrator actor. Here the orch node waits for the orch actor to prepare for a new
   * iteration of the benchmark. Once [[OrchestratorReady]] is received, the node starts a timer.
   */
  private def waitForOrchestrator(
                                   workerNodes: Map[String, ActorRef[MetaProtocol]],
                                   info: IterationInfo
                                 ): Behavior[MetaProtocol] =
    Behaviors.receive { (_, msg) =>
      msg match {
        case OrchestratorReady() =>
          info.readyLatch.countDown()
          waitForIterationCompletion(workerNodes, info)
      }
    }

  /** The orchestrator waits to receive [[OrchestratorDone]] and decides whether to do another
   * iteration.
   */
  private def waitForIterationCompletion(
                                          workerNodes: Map[String, ActorRef[MetaProtocol]],
                                          info: IterationInfo
                                        ): Behavior[MetaProtocol] =
    Behaviors.receive { (_, msg) =>
      msg match {
        case OrchestratorDone(results, filename) =>
          info.doneLatch.countDown()
          for ((_, worker) <- workerNodes)
            worker ! IterationDone()
          Behaviors.stopped
      }
    }
}
