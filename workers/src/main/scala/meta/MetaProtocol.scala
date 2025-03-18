package meta

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.receptionist.Receptionist
import org.apache.pekko.uigc.actor.typed.RemoteSpawner
import randomworkers.Protocol

/** The meta protocol is used to set up and tear down each instance of the benchmark. */
trait MetaProtocol extends CborSerializable

case class WorkerJoinedMessage(
  role: String,
  ref: ActorRef[MetaProtocol],
  rootActor: ActorRef[RemoteSpawner.Command[Protocol]]
) extends MetaProtocol

case class ReceptionistListing(listing: Receptionist.Listing) extends MetaProtocol

case class OrchestratorReady() extends MetaProtocol

case class OrchestratorDone(results: String = null, filename: String = null) extends MetaProtocol

case class IterationDone() extends MetaProtocol
