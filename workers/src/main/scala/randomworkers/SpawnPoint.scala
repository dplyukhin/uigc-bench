package randomworkers

import common.{ClusterBenchmark, ClusterProtocol, OrchestratorReady}
import org.apache.pekko.actor.typed
import org.apache.pekko.uigc.actor.typed.{RemoteSpawner, unmanaged}

object SpawnPoint {

  /**
   * Entry point for the "lead" spawn point, which runs on the orchestrator node.
   * The behavior does the following:
   * 1. Notify the benchmark actor that we're ready to start.
   * 2. Spawn the lead manager.
   *
   * @param benchmark A reference to the benchmark actor, which needs to be signalled when the benchmark is done.
   * @param workerNodes A map of worker node names to their actor references.
   * @param isWarmup Whether this is a warmup run.
   */
  def leader(
              benchmark: unmanaged.ActorRef[ClusterProtocol[RemoteSpawner.Command[Protocol]]],
              workerNodes: Map[String, unmanaged.ActorRef[RemoteSpawner.Command[Protocol]]],
              isWarmup: Boolean
  ): unmanaged.Behavior[RemoteSpawner.Command[Protocol]] =
    typed.scaladsl.Behaviors.setup[RemoteSpawner.Command[Protocol]] { ctx =>
      benchmark ! OrchestratorReady()

      // Spawn the manager actor.
      ctx.spawn(Manager.leadManager(benchmark, workerNodes.values, isWarmup), "manager0")

      // Become a spawn point, even though this node shouldn't be asked to spawn anything.
      SpawnPoint()
    }

  /**
   * Entry point for the "follower" spawn point, which runs on the worker nodes and spawns
   * a manager when asked to do so.
   */
  def follower(): unmanaged.Behavior[RemoteSpawner.Command[Protocol]] =
    SpawnPoint()

  private def apply(): unmanaged.Behavior[RemoteSpawner.Command[Protocol]] =
    RemoteSpawner(Map(
      "followerManager" ->
        (ctx => {
          val config = new Config()
          new Manager(ctx, config, Nil, null, isWarmup = false)
        })
    ))
}