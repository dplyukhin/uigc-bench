package randomworkers

import akka.actor.typed.scaladsl
import akka.actor.typed.scaladsl.TimerScheduler
import com.typesafe.config.ConfigFactory
import common.{CborSerializable, ClusterBenchmark}
import edu.illinois.osl.akka.gc.interfaces.{Message, NoRefs, RefobLike}
import edu.illinois.osl.akka.gc.{AbstractBehavior, ActorContext, ActorFactory, ActorRef, Behavior, Behaviors, unmanaged}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

object RandomWorkers {

  trait Protocol extends CborSerializable with Message
  private case class LearnPeers(peers: Seq[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[RefobLike[Nothing]] = peers
  }
  private case object Ping extends Protocol with NoRefs
  private case class Work(work: Array[Short]) extends Protocol with NoRefs
  private case class Acquaint(workers: Seq[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[RefobLike[Nothing]] = workers
  }

  private class Config() {
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
      Array.tabulate[Byte](rng.nextInt(size))(i => i.toByte)

    def select[T](items: Iterable[T]): T = {
      val i = rng.nextInt(0, items.size)
      items.view.slice(i, i + 1).head
    }

    def select[T](items: Iterable[T], bound: Int): Iterable[T] = {
      if (items.isEmpty) return Nil
      val numItems = rng.nextInt(0, bound + 1)
      (1 to numItems).map(_ => select(items))
    }

    def randNat(bound: Int): Int =
      rng.nextInt(0, bound)
  }

  private object Manager {

    def managerManager(
               benchmark: unmanaged.ActorRef[ClusterBenchmark.Protocol[Protocol]],
               workerNodes: Map[String, unmanaged.ActorRef[Protocol]],
               isWarmup: Boolean
             ): unmanaged.Behavior[Protocol] = {
      scaladsl.Behaviors.setup[Protocol] { ctx =>
        // This actor manages the lead manager; it spawns the manager and then
        // sends every manager a reference to every other manager.
        benchmark ! ClusterBenchmark.OrchestratorReady()

        val leader = ctx.spawnAnonymous(manager())

        val peers = workerNodes.values.toSeq :+ leader
        for (peer <- peers)
          peer ! LearnPeers(peers.filter(_ != peer))

        scaladsl.Behaviors.receiveMessage[Protocol] { _ =>
          scaladsl.Behaviors.same
        }
      }
    }

    def manager(): unmanaged.Behavior[Protocol] =
      Behaviors.withTimers[Protocol] { timers =>
        val config = new Config()
        timers.startTimerAtFixedRate((), Ping, (1000000000 / config.reqsPerSecond).nanos)

        Behaviors.setupRoot[Protocol] { ctx =>
          new ManagerActor(ctx, timers, config)
        }
      }

    private class ManagerActor(ctx: ActorContext[Protocol], timers: TimerScheduler[Protocol], config: Config) extends AbstractBehavior[Protocol](ctx) {

      private val rng = new Random()
      private var peers: Seq[ActorRef[Protocol]] = Seq()
      private val localWorkers: mutable.HashSet[ActorRef[Protocol]] = mutable.HashSet()
      private val remoteWorkers: mutable.HashSet[ActorRef[Protocol]] = mutable.HashSet()

      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        case LearnPeers(peers) =>
          this.peers = peers
          this

        case Acquaint(workers) =>
          remoteWorkers.addAll(workers)
          this

        case Ping =>
          if (rng.roll(config.managerProbSpawn)) {
            for (i <- 1 to rng.randNat(config.maxSpawnsInOneTurn))
              localWorkers.add(ctx.spawnAnonymous(Worker()))
          }
          if (rng.roll(config.managerProbLocalSend) && localWorkers.nonEmpty) {
            val work = rng.genData(config.maxWorkSizeBytes)
            for (i <- 1 to rng.randNat(config.maxSendsInOneTurn))
              rng.select(localWorkers) ! Work(work)
          }
          if (rng.roll(config.managerProbRemoteSend) && remoteWorkers.nonEmpty) {
            val work = rng.genData(config.maxWorkSizeBytes)
            for (i <- 1 to rng.randNat(config.maxSendsInOneTurn))
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

  object Worker {
    def apply(): ActorFactory[Protocol] =
      Behaviors.setup[Protocol] { ctx =>
        new WorkerActor(ctx)
      }

    private class WorkerActor(ctx: ActorContext[Protocol]) extends AbstractBehavior[Protocol](ctx) {
      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        case Ping => this
      }
    }
  }

  def main(args: Array[String]): Unit =
    ClusterBenchmark(
      Manager.managerManager,
      Map(
        "worker1" -> Manager.manager(),
        "worker2" -> Manager.manager(),
      )
    ).runBenchmark(args)
}