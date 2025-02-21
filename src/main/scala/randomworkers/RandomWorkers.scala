package randomworkers

import org.apache.pekko.actor.typed
import com.typesafe.config.ConfigFactory
import common.ClusterBenchmark.OrchestratorDone
import common.ClusterBenchmark
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.actor.typed.{Signal, PostStop}
import org.apache.pekko.uigc.actor.typed.scaladsl._
import randomworkers.jfr.AppMsgSerialization

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import java.util.concurrent.ConcurrentLinkedQueue
import java.io.BufferedWriter
import java.io.FileWriter

object RandomWorkers {

  /** The main entry point for the benchmark. */
  def main(args: Array[String]): Unit = {
    // Each node has a manager that spawns workers and sends them work.
    // Here we create the benchmark, passing in behaviors for the "leader" manager and for the "follower" managers (if they exist.)
    val bench = ClusterBenchmark[RemoteSpawner.Command[Protocol]](
      Manager.leader,
      Map(
        // TODO Add a flag so I don't have to comment this out manually
        //"manager1" -> Manager.spawnPoint(),
        //"manager2" -> Manager.spawnPoint()
      )
    )
    bench.runBenchmark(args)
  }

  trait Protocol extends Serializable with Message

  private class Random() {

    private val rng = new java.util.Random(System.currentTimeMillis())

    def roll(probability: Double): Boolean =
      rng.nextDouble() < probability

    def genData(size: Int): List[Int] =
      List.tabulate(rng.nextInt(size))(i => i)

    def selectDistinct[T](items: mutable.ArrayBuffer[T], bound: Int, chosen: Set[Any] = Set()): Iterable[T] = {
      if (items.isEmpty) return Nil
      if (bound == 0) return chosen.asInstanceOf[Iterable[T]]
      val item = select(items)
      if (chosen contains item)
        selectDistinct(items, bound - 1, chosen)
      else
        selectDistinct(items, bound - 1, chosen + item)
    }

    def select[T](items: mutable.ArrayBuffer[T], bound: Int): Iterable[T] = {
      if (items.isEmpty) return Nil
      val numItems = rng.nextInt(bound + 1)
      (1 to numItems).map(_ => select(items))
    }

    def select[T](items: mutable.ArrayBuffer[T]): T = {
      val i = rng.nextInt(items.size)
      items(i)
    }

    def randNat(bound: Int): Int =
      rng.nextInt(bound)
  }

  private case class LearnPeers(peers: mutable.ArrayBuffer[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[ActorRef[Nothing]] = peers
  }

  private case class Work(work: List[Int]) extends Protocol with NoRefs

  private case class Acquaint(workers: Seq[ActorRef[Protocol]]) extends Protocol {
    override def refs: Iterable[ActorRef[Nothing]] = workers
  }

  private case class Query(n: Int, master: ActorRef[Protocol]) extends Protocol {
    override def refs: Iterable[ActorRef[Nothing]] = Some(master)
  }

  private case class QueryResponse(n: Int) extends Protocol with NoRefs

  private class Config() {
    private val config = ConfigFactory.load("random-workers")
    val jvmGCFrequency = config.getInt("random-workers.jvm-gc-frequency")
    val reqsPerSecond = config.getInt("random-workers.reqs-per-second")
    val maxWorkSizeBytes = config.getInt("random-workers.max-work-size-in-bytes")
    val maxAcqsInOneMsg = config.getInt("random-workers.max-acqs-per-msg")
    val maxSendsInOneTurn = config.getInt("random-workers.max-sends-per-turn")
    val maxSpawnsInOneTurn = config.getInt("random-workers.max-spawns-per-turn")
    val maxDeactivatedInOneTurn = config.getInt("random-workers.max-deactivated-per-turn")
    val managerMaxAcquaintances = config.getInt("random-workers.manager-max-acquaintances")
    val totalQueries = config.getInt("random-workers.total-queries")
    val queryTimesFile = config.getString("random-workers.query-times-file")
    val lifeTimesFile = config.getString("random-workers.life-times-file")
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
  }

  private def remove(ref: ActorRef[Protocol], buf: mutable.ArrayBuffer[ActorRef[Protocol]]): Unit = {
    val i = buf.indexOf(ref)
    if (i != -1)
      buf.remove(i)
  }

  private def isRemote(actor: ActorRef[Nothing])(implicit context: ActorContext[Protocol]): Boolean = {
    actor.path.address != context.system.address
  }

  private def sendWorkMsg(recipient : ActorRef[Protocol], work : List[Int])(implicit context: ActorContext[Protocol]) : Unit = {
    recipient ! Work(work)
    if (isRemote(recipient)) {
      val metrics = new AppMsgSerialization()
      metrics.size += recipient.toString.length
      metrics.size = work.size * 4
      metrics.commit()
    }
  }

  private def sendAcquaintMsg(recipient : ActorRef[Protocol], workers : Seq[ActorRef[Protocol]])(implicit context: ActorContext[Protocol]) : Unit = {
    recipient ! Acquaint(workers)
    if (isRemote(recipient)) {
      val metrics = new AppMsgSerialization()
      metrics.size += recipient.toString.length
      metrics.size += workers.map(_.toString.length).sum
      metrics.commit()
    }
  }


  object Worker {
    def apply(config: Config): ActorFactory[Protocol] =
      Behaviors.setup[Protocol] { ctx =>
        new WorkerActor(ctx, config)
      }

    private class WorkerActor(ctx: ActorContext[Protocol], config: Config)
        extends AbstractBehavior[Protocol](ctx) {
      private val rng = new Random()
      private val acquaintances: mutable.ArrayBuffer[ActorRef[Protocol]] = new mutable.ArrayBuffer()
      private var state: List[Int] = List.tabulate(config.maxWorkSizeBytes / 4)(i => i)
      private val creationTime = System.nanoTime()

      override def onSignal: PartialFunction[Signal,Behavior[Protocol]] = {
        case PostStop =>
          val killTime = System.nanoTime()
          val lifeTimeMillis = (killTime - creationTime) / 1_000_000
          Manager.actorLifeTimes.add(lifeTimeMillis)
          this
      }

      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        case Acquaint(workers) =>
          acquaintances.addAll(workers)
          this

        case Query(n, master) =>
          master ! QueryResponse(n)
          this

        case Work(work) =>
          state = state.zip(work).map{case (a,b) => a + b}
          if (rng.roll(config.workerProbSpawn)) {
            for (_ <- 1 to rng.randNat(config.maxSpawnsInOneTurn))
              acquaintances.append(ctx.spawnAnonymous(Worker(config)))
          }
          if (rng.roll(config.workerProbSend) && acquaintances.nonEmpty) {
            val work = state
            for (_ <- 1 to rng.randNat(config.maxSendsInOneTurn)) {
              val recipient = rng.select(acquaintances)
              sendWorkMsg(recipient, work)
            }
          }
          if (rng.roll(config.workerProbAcquaint) && acquaintances.nonEmpty) {
            val acqs = rng.select(acquaintances, config.maxAcqsInOneMsg).toSeq
            val owner = rng.select(acquaintances)
            val refs = acqs.map(acq => ctx.createRef(acq, owner))
            sendAcquaintMsg(owner, refs)
          }
          if (rng.roll(config.workerProbDeactivate)) {
            val locals = rng.selectDistinct(acquaintances, config.maxDeactivatedInOneTurn).toSeq
            for (worker <- locals)
              remove(worker, acquaintances)
          }
          if (rng.roll(config.workerProbDeactivateAll)) {
            acquaintances.clear()
          }
          this
      }
    }
  }

  private def dumpMeasurements(results: String, filename: String): Unit = {
    if (filename == null) {
      println("ERROR: Missing filename to dump iteration-specific measurements")
    } else {
      println(s"Writing measurements to $filename")
      val writer = new BufferedWriter(new FileWriter(filename, true))
      writer.write(results)
      writer.close()
    }
  }

  private case object Ping extends Protocol with NoRefs
  private case object TriggerGC extends Protocol with NoRefs

  private object Manager {

    var actorLifeTimes: ConcurrentLinkedQueue[Long] = new ConcurrentLinkedQueue[Long]()

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
        benchmark: unmanaged.ActorRef[ClusterBenchmark.Protocol[RemoteSpawner.Command[Protocol]]],
        workerNodes: Map[String, unmanaged.ActorRef[RemoteSpawner.Command[Protocol]]],
        isWarmup: Boolean
    ): unmanaged.Behavior[RemoteSpawner.Command[Protocol]] =
      typed.scaladsl.Behaviors.setup[RemoteSpawner.Command[Protocol]] { ctx =>
        benchmark ! ClusterBenchmark.OrchestratorReady()

        // Spawn the manager actor.
        ctx.spawn(leadManager(benchmark, workerNodes.values, isWarmup), "manager0")

        // Become a spawn point, even though this node shouldn't be asked to spawn anything.
        spawnPoint(isWarmup)
      }

    /**
     * Entry point for the "lead" manager. This manager receives Ping messages at a fixed rate.
     * It's also responsible for spawning managers at the worker nodes.
     */
    private def leadManager(
        benchmark: unmanaged.ActorRef[ClusterBenchmark.Protocol[RemoteSpawner.Command[Protocol]]],
        workerNodes: Iterable[unmanaged.ActorRef[RemoteSpawner.Command[Protocol]]],
        isWarmup: Boolean
    ): unmanaged.Behavior[Protocol] =
      Behaviors.withTimers[Protocol] { timers =>
        val config = new Config()
        timers.startTimerAtFixedRate(Ping, Ping, (1000000000 / config.reqsPerSecond).nanos)
        if (config.jvmGCFrequency > 0)
          timers.startTimerAtFixedRate(TriggerGC, TriggerGC, config.jvmGCFrequency.millis)

        Behaviors.setupRoot[Protocol] { ctx =>
          new ManagerActor(ctx, config, workerNodes, benchmark, isWarmup)
        }
      }

    /**
     * Entry point for a spawn point. A spawn point will spawn a manager actor when asked.
     */
    def spawnPoint(isWarmup: Boolean): unmanaged.Behavior[RemoteSpawner.Command[Protocol]] =
      RemoteSpawner(Map(
        "followerManager" ->
          (ctx => {
            val config = new Config()
            new ManagerActor(ctx, config, Nil, null, isWarmup)
          })
      ))

    private class ManagerActor(
        ctx: ActorContext[Protocol],
        config: Config,
        workerNodes: Iterable[unmanaged.ActorRef[RemoteSpawner.Command[Protocol]]],
        benchmark: unmanaged.ActorRef[ClusterBenchmark.Protocol[RemoteSpawner.Command[Protocol]]],
        isWarmup: Boolean
    ) extends AbstractBehavior[Protocol](ctx) {

      private val rng = new Random()
      private val localWorkers: mutable.ArrayBuffer[ActorRef[Protocol]] = mutable.ArrayBuffer()
      private val remoteWorkers: mutable.ArrayBuffer[ActorRef[Protocol]] = mutable.ArrayBuffer()
      private var peers: mutable.ArrayBuffer[ActorRef[Protocol]] = mutable.ArrayBuffer()
      private var queryID: Int = 0
      private var queriesRemaining: Int = 0
      private val queryStartTimes: Array[Long] = Array.fill[Long](config.totalQueries)(-1)
      private val queryEndTimes: Array[Long] = Array.fill[Long](config.totalQueries)(-2)
      private val isLeader = benchmark != null
      private var done = false

      // If this is the leader manager (i.e. the manager on the orchestrator node), 
      // spawn the other managers and send those managers references to one another.
      if (workerNodes.nonEmpty) {
        peers = mutable.ArrayBuffer.from(
          for ((node, _) <- workerNodes.zipWithIndex)
            yield ctx.spawnRemote("followerManager", node)
        )
        for (peer <- peers) {
          val refs =
            (peers :+ ctx.self).filter(_ != peer).map(target => ctx.createRef(target, peer))
          peer ! LearnPeers(refs)
        }
      }

      override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
        // If this manager is a follower, it will receive this message giving it a list of its peers.
        case LearnPeers(peers) =>
          this.peers = peers
          this

        case Acquaint(workers) =>
          remoteWorkers.addAll(workers)
          runActions()
          this

        // The benchmark ends after `config.totalQueries` queries have been answered.
        case QueryResponse(id) =>
          queryEndTimes(id) = System.nanoTime()
          queriesRemaining -= 1
          if (queriesRemaining == 0 && queryID == config.totalQueries) {
            val queryTimes = Array.tabulate[Long](config.totalQueries) { i =>
              queryEndTimes(i) - queryStartTimes(i)
            }
            val lifeTimes = Manager.actorLifeTimes.toArray().map(_.asInstanceOf[Long])
            benchmark ! OrchestratorDone()
            if (!isWarmup) {
              dumpMeasurements(queryTimes.mkString("\n"), config.queryTimesFile)
              dumpMeasurements(lifeTimes.mkString("\n"), config.lifeTimesFile)
            }
            Manager.actorLifeTimes = new ConcurrentLinkedQueue[Long]()
            done = true
            localWorkers.clear()
            remoteWorkers.clear()
          }
          this

        // If this manager is the "lead" manager, it will get Ping messages at a fixed rate.
        case Ping =>
          runActions()
          this

        case TriggerGC =>
          System.gc()
          this
      }

      private def runActions(): Unit = {
        if (done) return
        if (rng.roll(config.managerProbSpawn)) {
          for (_ <- 1 to rng.randNat(config.maxSpawnsInOneTurn))
            localWorkers.append(ctx.spawnAnonymous(Worker(config)))
        }
        if (rng.roll(config.managerProbLocalSend) && localWorkers.nonEmpty) {
          val work = rng.genData(config.maxWorkSizeBytes / 4)
          for (_ <- 1 to rng.randNat(config.maxSendsInOneTurn)) {
            val recipient = rng.select(localWorkers)
            sendWorkMsg(recipient, work)
          }
        }
        if (rng.roll(config.managerProbRemoteSend) && remoteWorkers.nonEmpty) {
          val work = rng.genData(config.maxWorkSizeBytes / 4)
          for (_ <- 1 to rng.randNat(config.maxSendsInOneTurn)) {
            val recipient = rng.select(remoteWorkers)
            sendWorkMsg(recipient, work)
          }
        }
        if (rng.roll(config.managerProbLocalAcquaint) && localWorkers.nonEmpty) {
          val acqs = rng.select(localWorkers, config.maxAcqsInOneMsg).toSeq
          val owner = rng.select(localWorkers)
          val refs = acqs.map(acq => ctx.createRef(acq, owner))
          sendAcquaintMsg(owner, refs)
        }
        if (rng.roll(config.managerProbRemoteAcquaint) && localWorkers.nonEmpty && remoteWorkers.nonEmpty) {
          val acqs = rng.select(remoteWorkers, config.maxAcqsInOneMsg).toSeq
          val owner = rng.select(localWorkers)
          val refs = acqs.map(acq => ctx.createRef(acq, owner))
          sendAcquaintMsg(owner, refs)
        }
        if (rng.roll(config.managerProbPublishWorker) && peers.nonEmpty) {
          val acqs = rng.select(localWorkers, config.maxAcqsInOneMsg).toSeq
          val peer = rng.select(peers)
          val refs = acqs.map(acq => ctx.createRef(acq, peer))
          sendAcquaintMsg(peer, refs)
        }
        if (rng.roll(config.managerProbQuery) && isLeader && localWorkers.nonEmpty && queryID < config.totalQueries) {
          val worker = rng.select(localWorkers)
          worker ! Query(queryID, ctx.createRef(ctx.self, worker))
          queryStartTimes(queryID) = System.nanoTime()
          queryID += 1
          queriesRemaining += 1
        }
        if (rng.roll(config.managerProbDeactivate)) {
          val locals = rng.selectDistinct(localWorkers, config.maxDeactivatedInOneTurn).toSeq
          val remotes = rng.selectDistinct(remoteWorkers, config.maxDeactivatedInOneTurn).toSeq
          for (worker <- locals)
            remove(worker, localWorkers)
          for (worker <- remotes) {
            remove(worker, remoteWorkers)
          }
        }
        if (rng.roll(config.managerProbDeactivateAll)
          || localWorkers.size + remoteWorkers.size > config.managerMaxAcquaintances) {
          localWorkers.clear()
          remoteWorkers.clear()
        }
      }

    }
  }
}
