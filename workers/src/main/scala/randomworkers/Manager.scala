package randomworkers

import meta.{MetaProtocol, OrchestratorDone}
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl.{ActorContext, Behaviors}
import randomworkers.Benchmark._

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object Manager {

  var actorLifeTimes: ConcurrentLinkedQueue[Long] = new ConcurrentLinkedQueue[Long]()

  /** Entry point for the "lead" manager. This manager receives Ping messages at a fixed rate. It's
    * also responsible for spawning managers at the worker nodes.
    */
  def leadManager(
      benchmark: unmanaged.ActorRef[MetaProtocol],
      workerNodes: Iterable[unmanaged.ActorRef[RemoteSpawner.Command[Protocol]]],
      isWarmup: Boolean
  ): unmanaged.Behavior[Protocol] =
    Behaviors.withTimers[Protocol] { timers =>
      val config = new Config()
      timers.startTimerAtFixedRate(Ping, Ping, (1000000000 / config.reqsPerSecond).nanos)
      if (config.jvmGCFrequency > 0)
        timers.startTimerAtFixedRate(TriggerGC, TriggerGC, config.jvmGCFrequency.millis)

      Behaviors.setupRoot[Protocol] { ctx =>
        new Manager(ctx, config, workerNodes, benchmark, isWarmup)
      }
    }

}

class Manager(
    ctx: ActorContext[Protocol],
    config: Config,
    workerNodes: Iterable[unmanaged.ActorRef[RemoteSpawner.Command[Protocol]]],
    benchmark: unmanaged.ActorRef[MetaProtocol],
    isWarmup: Boolean
) extends AbstractBehavior[Protocol](ctx) {

  private val rng                                                    = new Random()
  private val localWorkers: mutable.ArrayBuffer[ActorRef[Protocol]]  = mutable.ArrayBuffer()
  private val remoteWorkers: mutable.ArrayBuffer[ActorRef[Protocol]] = mutable.ArrayBuffer()
  private var peers: mutable.ArrayBuffer[ActorRef[Protocol]]         = mutable.ArrayBuffer()
  private var queryID: Int                                           = 0
  private var queriesRemaining: Int                                  = 0
  private val queryStartTimes: Array[Long] = Array.fill[Long](config.totalQueries)(-1)
  private val queryEndTimes: Array[Long]   = Array.fill[Long](config.totalQueries)(-2)
  private val isLeader                     = benchmark != null
  private var done                         = false

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

  def localWorkersForOwner(owner: ActorRef[Protocol]): mutable.ArrayBuffer[ActorRef[Protocol]] =
    if (config.acyclic)
      localWorkers.filter(worker => lessThan(owner, worker))
    else
      localWorkers

  def remoteWorkersForOwner(owner: ActorRef[Protocol]): mutable.ArrayBuffer[ActorRef[Protocol]] =
    if (config.acyclic)
      remoteWorkers.filter(worker => lessThan(owner, worker))
    else
      remoteWorkers

  override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
    // If this manager is a follower, it will receive this message giving it a list of its peers.
    case LearnPeers(peers) =>
      this.peers = peers
      this

    case Acquaint(workers) =>
      if (config.acyclic) {
        for (worker <- workers)
          if (!lessThan(ctx.self, worker))
            println("ERROR: Acyclic invariant violated in Manager.Acquaint")
      }
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
      val owner = rng.select(localWorkers)
      val acqs  = rng.select(localWorkersForOwner(owner), config.maxAcqsInOneMsg).toSeq
      val refs  = acqs.map(acq => ctx.createRef(acq, owner))
      sendAcquaintMsg(owner, refs)
    }
    if (
      rng.roll(config.managerProbRemoteAcquaint) && localWorkers.nonEmpty && remoteWorkers.nonEmpty
    ) {
      val owner = rng.select(localWorkers)
      val acqs  = rng.select(remoteWorkersForOwner(owner), config.maxAcqsInOneMsg).toSeq
      val refs  = acqs.map(acq => ctx.createRef(acq, owner))
      sendAcquaintMsg(owner, refs)
    }
    if (rng.roll(config.managerProbPublishWorker) && peers.nonEmpty) {
      val peer = rng.select(peers)
      val acqs = rng.select(localWorkersForOwner(peer), config.maxAcqsInOneMsg).toSeq
      val refs = acqs.map(acq => ctx.createRef(acq, peer))
      sendAcquaintMsg(peer, refs)
    }
    if (
      rng.roll(
        config.managerProbQuery
      ) && isLeader && localWorkers.nonEmpty && queryID < config.totalQueries
    ) {
      val worker = rng.select(localWorkers)
      worker ! Query(queryID, ctx.createRef(ctx.self, worker))
      queryStartTimes(queryID) = System.nanoTime()
      queryID += 1
      queriesRemaining += 1
    }
    if (rng.roll(config.managerProbDeactivate)) {
      val locals  = rng.selectDistinct(localWorkers, config.maxDeactivatedInOneTurn).toSeq
      val remotes = rng.selectDistinct(remoteWorkers, config.maxDeactivatedInOneTurn).toSeq
      for (worker <- locals)
        remove(worker, localWorkers)
      for (worker <- remotes)
        remove(worker, remoteWorkers)
    }
    if (
      rng.roll(config.managerProbDeactivateAll)
      || localWorkers.size + remoteWorkers.size > config.managerMaxAcquaintances
    ) {
      localWorkers.clear()
      remoteWorkers.clear()
    }
  }

}
