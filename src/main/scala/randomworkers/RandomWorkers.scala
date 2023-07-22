package randomworkers

import akka.actor.typed.{SpawnProtocol, scaladsl}
import com.typesafe.config.ConfigFactory
import common.ClusterBenchmark.OrchestratorDone
import common.{CborSerializable, ClusterBenchmark}
import edu.illinois.osl.akka.gc.interfaces.{Message, NoRefs, RefobLike}
import edu.illinois.osl.akka.gc._

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object RandomWorkers {

  def main(args: Array[String]): Unit =
    ClusterBenchmark[SpawnProtocol.Command](
      Manager.leader,
      Map(
        "worker1" -> Manager.follower(),
        "worker2" -> Manager.follower()
      )
    ).runBenchmark(args)

  trait Protocol extends CborSerializable with Message

  class Random() {

    private val rng = new java.util.Random(System.currentTimeMillis())

    def roll(probability: Double): Boolean =
      rng.nextDouble(1.0) < probability

    def genData(size: Int): Array[Byte] =
      Array.tabulate[Byte](rng.nextInt(size))(i => i.toByte)

    def select[T](items: Iterable[T], bound: Int): Iterable[T] = {
      if (items.isEmpty) return Nil
      val numItems = rng.nextInt(0, bound + 1)
      (1 to numItems).map(_ => select(items))
    }

    def select[T](items: Iterable[T]): T = {
      val i = rng.nextInt(0, items.size)
      items.view.slice(i, i + 1).head
    }

    def randNat(bound: Int): Int =
      rng.nextInt(0, bound)
  }

  private case class LearnPeers(peers: Seq[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[RefobLike[Nothing]] = peers
  }

  private case class Work(work: Array[Byte]) extends Protocol with NoRefs

  private case class Acquaint(workers: Seq[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[RefobLike[Nothing]] = workers
  }

  private case class Query(n: Int, master: ActorRef[Protocol]) extends Protocol {
    override def refs: Iterable[RefobLike[Nothing]] = Some(master)
  }

  private case class QueryResponse(n: Int) extends Protocol with NoRefs

  private class Config() {
    val reqsPerSecond = config.getInt("random-workers.reqs-per-second")
    val maxWorkSizeBytes = config.getInt("random-workers.max-work-size-in-bytes")
    val maxAcqsInOneMsg = config.getInt("random-workers.max-acqs-per-msg")
    val maxSendsInOneTurn = config.getInt("random-workers.max-sends-per-turn")
    val maxSpawnsInOneTurn = config.getInt("random-workers.max-spawns-per-turn")
    val maxDeactivatedInOneTurn = config.getInt("random-workers.max-deactivated-per-turn")
    val totalQueries = config.getInt("random-workers.total-queries")
    val queryTimesFile = config.getString("random-workers.query-times-file")
    val managerProbSpawn = config.getDouble("random-workers.mgr-probabilities.spawn")
    val managerProbLocalSend = config.getDouble("random-workers.mgr-probabilities.local-send")
    val managerProbRemoteSend = config.getDouble("random-workers.mgr-probabilities.remote-send")
    val managerProbLocalAcquaint = config.getDouble("random-workers.mgr-probabilities.local-acquaint")
    val managerProbRemoteAcquaint = config.getDouble("random-workers.mgr-probabilities.remote-acquaint")
    val managerProbPublishWorker = config.getDouble("random-workers.mgr-probabilities.publish-worker")
    val managerProbDeactivate = config.getDouble("random-workers.mgr-probabilities.deactivate")
    val managerProbDeactivateAll = config.getDouble("random-workers.mgr-probabilities.deactivate-all")
    val managerProbQuery = config.getDouble("random-workers.mgr-probabilities.query")
    val workerProbSpawn = config.getDouble("random-workers.wrk-probabilities.spawn")
    val workerProbSend = config.getDouble("random-workers.wrk-probabilities.send")
    val workerProbAcquaint = config.getDouble("random-workers.wrk-probabilities.acquaint")
    val workerProbDeactivate = config.getDouble("random-workers.wrk-probabilities.deactivate")
    val workerProbDeactivateAll = config.getDouble("random-workers.wrk-probabilities.deactivate-all")
    private val config = ConfigFactory.load("random-workers")
  }

  object Worker {
    def apply(config: Config, rng: Random): ActorFactory[Protocol] =
      Behaviors.setup[Protocol] { ctx =>
        new WorkerActor(ctx, config, rng)
      }

    private class WorkerActor(ctx: ActorContext[Protocol], config: Config, rng: Random)
        extends AbstractBehavior[Protocol](ctx) {
      private val acquaintances: mutable.HashSet[ActorRef[Protocol]] = mutable.HashSet()
      private val state: Array[Byte] = Array.tabulate[Byte](config.maxWorkSizeBytes)(i => i.toByte)

      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        case Acquaint(workers) =>
          acquaintances.addAll(workers)
          this
        case Work(work) =>
          for (i <- work.indices)
            state(i) = (state(i) * work(i)).toByte
          if (rng.roll(config.workerProbSpawn)) {
            for (i <- 1 to rng.randNat(config.maxSpawnsInOneTurn))
              acquaintances.add(ctx.spawnAnonymous(Worker(config, rng)))
          }
          if (rng.roll(config.workerProbSend) && acquaintances.nonEmpty) {
            val work = state
            for (i <- 1 to rng.randNat(config.maxSendsInOneTurn))
              rng.select(acquaintances) ! Work(work)
          }
          if (rng.roll(config.workerProbAcquaint) && acquaintances.nonEmpty) {
            val acqs = rng.select(acquaintances, config.maxAcqsInOneMsg).toSeq
            val owner = rng.select(acquaintances)
            val refs = acqs.map(acq => ctx.createRef(acq, owner))
            owner ! Acquaint(refs)
          }
          if (rng.roll(config.workerProbDeactivate)) {
            val locals = rng.select(acquaintances, config.maxDeactivatedInOneTurn).toSeq
            ctx.release(locals)
            for (worker <- locals)
              acquaintances.remove(worker)
          }
          if (rng.roll(config.workerProbDeactivateAll)) {
            ctx.release(acquaintances)
            acquaintances.clear()
          }
          this
      }
    }
  }

  private case object Ping extends Protocol with NoRefs

  private object Manager {

    def leader(
        benchmark: unmanaged.ActorRef[ClusterBenchmark.Protocol[SpawnProtocol.Command]],
        workerNodes: Map[String, unmanaged.ActorRef[SpawnProtocol.Command]],
        isWarmup: Boolean
    ): unmanaged.Behavior[SpawnProtocol.Command] =
      scaladsl.Behaviors.setup[SpawnProtocol.Command] { ctx =>
        benchmark ! ClusterBenchmark.OrchestratorReady()

        ctx.spawn(leadManager(benchmark, workerNodes.values), "manager0")

        SpawnProtocol()
      }

    private def leadManager(
        benchmark: unmanaged.ActorRef[ClusterBenchmark.Protocol[SpawnProtocol.Command]],
        workerNodes: Iterable[unmanaged.ActorRef[SpawnProtocol.Command]]
    ): unmanaged.Behavior[Protocol] =
      Behaviors.withTimers[Protocol] { timers =>
        val config = new Config()
        timers.startTimerAtFixedRate((), Ping, (1000000000 / config.reqsPerSecond).nanos)

        Behaviors.setupRoot[Protocol] { ctx =>
          new ManagerActor(ctx, config, workerNodes, benchmark)
        }
      }

    def follower(): unmanaged.Behavior[SpawnProtocol.Command] =
      scaladsl.Behaviors.setup[SpawnProtocol.Command] { _ =>
        SpawnProtocol()
      }

    private def followerManager(): ActorFactory[Protocol] =
      Behaviors.setup[Protocol] { ctx =>
        val config = new Config()
        new ManagerActor(ctx, config, Nil, null)
      }

    private class ManagerActor(
        ctx: ActorContext[Protocol],
        config: Config,
        workerNodes: Iterable[unmanaged.ActorRef[SpawnProtocol.Command]],
        benchmark: unmanaged.ActorRef[ClusterBenchmark.Protocol[SpawnProtocol.Command]]
    ) extends AbstractBehavior[Protocol](ctx) {

      private val rng = new Random()
      private val localWorkers: mutable.HashSet[ActorRef[Protocol]] = mutable.HashSet()
      private val remoteWorkers: mutable.HashSet[ActorRef[Protocol]] = mutable.HashSet()
      private var peers: Seq[ActorRef[Protocol]] = Seq()
      private var queryID: Int = 0
      private var queriesRemaining: Int = 0
      private val queryStartTimes: Array[Long] = Array.fill[Long](config.totalQueries)(-1)
      private val queryEndTimes: Array[Long] = Array.fill[Long](config.totalQueries)(-2)

      // Spawn the other managers and send those managers references to one another
      if (workerNodes.nonEmpty) {
        peers = (for ((node, i) <- workerNodes.zipWithIndex)
          yield ctx.spawnRemote(followerManager(), node, s"manager${i + 1}")).toSeq
        for (peer <- peers) {
          val refs =
            (peers :+ ctx.self).filter(_ != peer).map(target => ctx.createRef(target, peer))
          peer ! LearnPeers(refs)
        }
      }

      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        case LearnPeers(peers) =>
          this.peers = peers
          this

        case Acquaint(workers) =>
          remoteWorkers.addAll(workers)
          this

        case QueryResponse(id) =>
          queryEndTimes(id) = System.nanoTime()
          queriesRemaining -= 1
          if (queriesRemaining == 0 && queryID == config.totalQueries) {
            val queryTimes = Array.tabulate[Long](config.totalQueries) { i =>
              queryEndTimes(i) - queryStartTimes(i)
            }
            benchmark ! OrchestratorDone(results = queryTimes.mkString("\n"), filename = config.queryTimesFile)
          }
          this

        case Ping =>
          if (rng.roll(config.managerProbSpawn)) {
            for (_ <- 1 to rng.randNat(config.maxSpawnsInOneTurn))
              localWorkers.add(ctx.spawnAnonymous(Worker(config, rng)))
          }
          if (rng.roll(config.managerProbLocalSend) && localWorkers.nonEmpty) {
            val work = rng.genData(config.maxWorkSizeBytes)
            for (_ <- 1 to rng.randNat(config.maxSendsInOneTurn))
              rng.select(localWorkers) ! Work(work)
          }
          if (rng.roll(config.managerProbRemoteSend) && remoteWorkers.nonEmpty) {
            val work = rng.genData(config.maxWorkSizeBytes)
            for (_ <- 1 to rng.randNat(config.maxSendsInOneTurn))
              rng.select(remoteWorkers) ! Work(work)
          }
          if (rng.roll(config.managerProbLocalAcquaint) && localWorkers.nonEmpty) {
            val acqs = rng.select(localWorkers, config.maxAcqsInOneMsg).toSeq
            val owner = rng.select(localWorkers)
            val refs = acqs.map(acq => ctx.createRef(acq, owner))
            owner ! Acquaint(refs)
          }
          if (rng.roll(config.managerProbRemoteAcquaint) && localWorkers.nonEmpty) {
            val acqs = rng.select(remoteWorkers, config.maxAcqsInOneMsg).toSeq
            val owner = rng.select(localWorkers)
            val refs = acqs.map(acq => ctx.createRef(acq, owner))
            owner ! Acquaint(refs)
          }
          if (rng.roll(config.managerProbPublishWorker) && peers.nonEmpty) {
            val acqs = rng.select(localWorkers, config.maxAcqsInOneMsg).toSeq
            val peer = rng.select(peers)
            val refs = acqs.map(acq => ctx.createRef(acq, peer))
            peer ! Acquaint(refs)
          }
          if (rng.roll(config.managerProbQuery) && localWorkers.nonEmpty && queryID < config.totalQueries) {
            val worker = rng.select(localWorkers)
            worker ! Query(queryID, ctx.createRef(ctx.self, worker))
            queryStartTimes(queryID) = System.nanoTime()
            queryID += 1
            queriesRemaining += 1
          }
          if (rng.roll(config.managerProbDeactivate)) {
            val locals = rng.select(localWorkers, config.maxDeactivatedInOneTurn).toSeq
            val remotes = rng.select(remoteWorkers, config.maxDeactivatedInOneTurn).toSeq
            ctx.release(locals)
            ctx.release(remotes)
            for (worker <- locals)
              localWorkers.remove(worker)
            for (worker <- remotes)
              remoteWorkers.remove(worker)
          }
          if (rng.roll(config.managerProbDeactivateAll)) {
            ctx.release(localWorkers)
            ctx.release(remoteWorkers)
            localWorkers.clear()
            remoteWorkers.clear()
          }
          this
      }
    }
  }
}
