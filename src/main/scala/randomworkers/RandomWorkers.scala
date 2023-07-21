package randomworkers

import akka.actor.typed.scaladsl.TimerScheduler
import com.typesafe.config.{Config, ConfigFactory}
import common.{CborSerializable, ClusterBenchmark}
import edu.illinois.osl.akka.gc.interfaces.{Message, NoRefs, RefobLike}
import edu.illinois.osl.akka.gc.{AbstractBehavior, ActorContext, ActorRef, Behavior, Behaviors, unmanaged}
import randomworkers.RandomWorkers.Manager.Cfg

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

object RandomWorkers {

  trait Protocol extends CborSerializable with Message
  private case class LearnPeers(peers: Seq[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[RefobLike[Nothing]] = peers
  }
  private case object NewWork extends Protocol with NoRefs
  private case class Acquaint(workers: Seq[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[RefobLike[Nothing]] = workers
  }

  private object Manager {
    class Cfg() {
      private val config = ConfigFactory.load("random-workers")
      val reqsPerSecond = config.getInt("random-workers.")
      val maxWorkSizeBytes = config.getInt("random-workers.")
      val maxWorkDuration = config.getInt("random-workers.")
      val maxAcqsInOneMsg = config.getInt("random-workers.")
      val maxSendsInOneTurn = config.getInt("random-workers.")
      val maxSpawnsInOneTurn = config.getInt("random-workers.")
      val maxDeactivatedInOneTurn = config.getInt("random-workers.")
      val managerProbSpawn = config.getDouble("random-workers.")
      val managerProbLocalSend = config.getDouble("random-workers.")
      val managerProbRemoteSend = config.getDouble("random-workers.")
      val managerProbLocalAcquaint = config.getDouble("random-workers.")
      val managerProbRemoteAcquaint = config.getDouble("random-workers.")
      val managerProbPublishWorker = config.getDouble("random-workers.")
      val managerProbDeactivate = config.getDouble("random-workers.")
      val managerProbDeactivateAll = config.getDouble("random-workers.")
      val workerProbSpawn = config.getDouble("random-workers.")
      val workerProbSend = config.getDouble("random-workers.")
      val workerProbAcquaint = config.getDouble("random-workers.")
      val workerProbDeactivate = config.getDouble("random-workers.")
      val workerProbDeactivateAll = config.getDouble("random-workers.")
      val workerProbRetainData = config.getDouble("random-workers.")
      val workerProbReleaseData = config.getDouble("random-workers.")
    }

    class Random() {

      private val rng = new java.util.Random(System.currentTimeMillis())

      def roll(probability: Double): Boolean =
        rng.nextDouble(1.0) < probability

      def genData(size: Int): Array[Byte] =
        Array.fill(rng.nextInt(size))(0)

      def select[T](items: Iterable[T]): T = {
        val i = rng.nextInt(0, items.size)
        items.view.slice(i, i + 1).head
      }

      def randNat(bound: Int): Int =
        rng.nextInt(0, bound)
    }

    def leader(
               benchmark: ActorRef[ClusterBenchmark.Protocol[Protocol]],
               workerNodes: Map[String, ActorRef[Protocol]],
               isWarmup: Boolean
             ): unmanaged.Behavior[Protocol] =
      Behaviors.setupRoot[Protocol] { ctx =>
        benchmark ! ClusterBenchmark.OrchestratorReady()

        val peers = workerNodes.values.toSeq :+ ctx.self
        for (worker <- workerNodes.values)
          worker ! LearnPeers(peers)

        startManager(peers)
      }

    private def startManager(peers: Seq[ActorRef[Protocol]]): unmanaged.Behavior[Protocol] =
      Behaviors.withTimers[Protocol] { timers =>
        val cfg = new Cfg()
        timers.startTimerAtFixedRate((), NewWork, (1000000000 / cfg.reqsPerSecond).nanos)
        Behaviors.setupRoot[Protocol](ctx =>
          new ManagerActor(ctx, timers, cfg)
        )
      }

    private class ManagerActor(ctx: ActorContext[Protocol], timers: TimerScheduler[Protocol], config: Cfg) extends AbstractBehavior[Protocol](ctx) {

      private val rng = new Random()
      private var peers: Seq[ActorRef[Protocol]] = Seq()
      private val localWorkers: ArrayBuffer[ActorRef[Protocol]] = new ArrayBuffer()
      private val remoteWorkers: ArrayBuffer[ActorRef[Protocol]] = new ArrayBuffer()

      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        case LearnPeers(peers) =>
          this.peers = peers
          this

        case Acquaint(workers) =>
          remoteWorkers.appendAll(workers)
          this

        case NewWork =>
          if (rng.roll(config.managerProbSpawn)) {
            for (i <- 1 to rng.randNat(config.maxSpawnsInOneTurn))
              localWorkers.append(ctx.spawnAnonymous(Worker()))
          }
          if (rng.roll(config.managerProbLocalSend) && remoteWorkers.nonEmpty) {
            // TODO Create work
            for (i <- 1 to rng.randNat(config.maxSendsInOneTurn))
              rng.select(remoteWorkers) ! NewWork
          }
          if (rng.roll(config.managerProbRemoteSend) && remoteWorkers.nonEmpty) {
            // TODO Create work
            for (i <- 1 to rng.randNat(config.maxSendsInOneTurn))
              rng.select(remoteWorkers) ! NewWork
          }
          if (rng.roll(config.managerProbLocalAcquaint) && localWorkers.nonEmpty) {
            val numAcqs = rng.randNat(config.maxAcqsInOneMsg + 1)
            val acqs = (1 to numAcqs).map(_ => rng.select(localWorkers))
            val owner = rng.select(localWorkers)
            // TODO Create refs
            owner ! Acquaint(acqs)
          }
          if (rng.roll(config.managerProbRemoteAcquaint) && remoteWorkers.nonEmpty && localWorkers.nonEmpty) {
            val numAcqs = rng.randNat(config.maxAcqsInOneMsg + 1)
            val acqs = (1 to numAcqs).map(_ => rng.select(remoteWorkers))
            val owner = rng.select(localWorkers)
            // TODO Create refs
            owner ! Acquaint(acqs)
          }
          if (rng.roll(config.managerProbPublishWorker) && localWorkers.nonEmpty) {
            val numAcqs = rng.randNat(config.maxAcqsInOneMsg + 1)
            val acqs = (1 to numAcqs).map(_ => rng.select(localWorkers))
            val peer = rng.select(peers)
            // TODO Create refs
            peer ! Acquaint(acqs)
          }
          if (rng.roll(config.managerProbDeactivate)) {
            val numDeacs = rng.randNat(config.maxDeactivatedInOneTurn + 1)
            for (_ <- 1 to numDeacs)
              () // TODO
          }
          if (rng.roll(config.managerProbDeactivateAll)) {
            localWorkers.clear()
            remoteWorkers.clear()
          }
          this
      }
    }

    private def manage(
                        config: Cfg,
                        rng: Random,
                        peers: Seq[ActorRef[Protocol]],
                        localWorkers: ArrayBuffer[ActorRef[Protocol]] = new ArrayBuffer(),
                        remoteWorkers: ArrayBuffer[ActorRef[Protocol]] = new ArrayBuffer(),
                      ): Behavior[Protocol] =
      Behaviors.receive { (ctx, msg) => msg match {
      }}
  }

  object Worker {
    def apply(): Behavior[Protocol] =
      Behaviors.setup[Protocol] { ctx =>
        new WorkerActor(ctx)
      }

    private class WorkerActor(ctx: ActorContext[Protocol]) extends AbstractBehavior[Protocol](ctx) {
      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        case NewWork => this
      }
    }
  }

  def main(args: Array[String]): Unit =
    ClusterBenchmark(
      Manager.leader,
      Map(
        "worker1" -> Manager.follower(),
        "worker2" -> Manager.follower(),
      )
    ).runBenchmark(args)
}