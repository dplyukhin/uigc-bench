package meta

import meta.Orchestrator.OrchestratorServiceKey
import org.apache.pekko.actor.typed.receptionist.Receptionist
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.uigc.actor.typed.RemoteSpawner
import randomworkers.{Protocol, SpawnPoint}

import java.util.concurrent.CountDownLatch

private object Worker {
  def apply(
      info: IterationInfo
  ): Behavior[MetaProtocol] = Behaviors.setup[MetaProtocol] { ctx =>
    val adapter = ctx.messageAdapter[Receptionist.Listing](ReceptionistListing.apply)
    ctx.system.receptionist ! Receptionist.Subscribe(OrchestratorServiceKey, adapter)
    val rootActor = ctx.spawnAnonymous(SpawnPoint.follower())
    info.readyLatch.countDown()
    waitForOrchestrator(rootActor, info)
  }

  /** Worker node waits to receive a reference to the orchestrator node. */
  private def waitForOrchestrator(
      rootActor: ActorRef[RemoteSpawner.Command[Protocol]],
      info: IterationInfo
  ): Behavior[MetaProtocol] =
    Behaviors.receive { (ctx, msg) =>
      msg match {
        case ReceptionistListing(OrchestratorServiceKey.Listing(listings)) =>
          listings.find(_ => true) match {
            case Some(orchestratorNode) =>
              orchestratorNode ! WorkerJoinedMessage(info.role, ctx.self, rootActor)
              prepareForTermination(info.doneLatch)
            case None =>
              waitForOrchestrator(rootActor, info)
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
