package randomworkers

import org.apache.pekko.actor.typed.{PostStop, Signal}
import org.apache.pekko.uigc.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.uigc.actor.typed.{AbstractBehavior, ActorFactory, ActorRef, Behavior}
import randomworkers.Benchmark.{lessThan, remove, sendAcquaintMsg, sendWorkMsg}

import scala.collection.mutable

object Worker {
  def apply(config: Config): ActorFactory[Protocol] =
    Behaviors.setup[Protocol] { ctx =>
      new Worker(ctx, config)
    }
}

class Worker(ctx: ActorContext[Protocol], config: Config) extends AbstractBehavior[Protocol](ctx) {

  private val rng                                                    = new Random()
  private val acquaintances: mutable.ArrayBuffer[ActorRef[Protocol]] = new mutable.ArrayBuffer()
  private var state: List[Int] = List.tabulate(config.maxWorkSizeBytes / 4)(i => i)
  private val creationTime     = System.nanoTime()

  def acquaintancesForOwner(owner: ActorRef[Protocol]): mutable.ArrayBuffer[ActorRef[Protocol]] =
    if (config.acyclic)
      acquaintances.filter(acq => lessThan(owner, acq))
    else
      acquaintances

  override def onSignal: PartialFunction[Signal, Behavior[Protocol]] = { case PostStop =>
    val killTime       = System.nanoTime()
    val lifeTimeMillis = (killTime - creationTime) / 1_000_000
    Manager.actorLifeTimes.add(lifeTimeMillis)
    this
  }

  override def onMessage(msg: Protocol): Behavior[Protocol] = msg match {
    case Acquaint(workers) =>
      if (config.acyclic) {
        for (worker <- workers)
          if (!lessThan(ctx.self, worker))
            println("ERROR: Acyclic invariant violated in Worker.Acquaint")
      }
      acquaintances.addAll(workers)
      this

    case Query(n, master) =>
      master ! QueryResponse(n)
      this

    case Work(work) =>
      state = state.zip(work).map { case (a, b) => a + b }
      if (rng.roll(config.workerProbSpawn)) {
        for (_ <- 1 to rng.randNat(config.maxSpawnsInOneTurn)) {
          val worker = ctx.spawnAnonymous(Worker(config))
          if (!lessThan(ctx.self, worker))
            println("ERROR: Acyclic invariant violated in Worker.spawn.")
          acquaintances.append(worker)
        }
      }
      if (rng.roll(config.workerProbSend) && acquaintances.nonEmpty) {
        val work = state
        for (_ <- 1 to rng.randNat(config.maxSendsInOneTurn)) {
          val recipient = rng.select(acquaintances)
          sendWorkMsg(recipient, work)
        }
      }
      if (rng.roll(config.workerProbAcquaint) && acquaintances.nonEmpty) {
        val owner = rng.select(acquaintances)
        val acqs  = rng.select(acquaintancesForOwner(owner), config.maxAcqsInOneMsg).toSeq
        val refs  = acqs.map(acq => ctx.createRef(acq, owner))
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
