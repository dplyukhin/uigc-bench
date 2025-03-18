package common

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.receptionist.Receptionist


trait ClusterProtocol[T] extends CborSerializable
case class WorkerJoinedMessage[T](
                                   role: String,
                                   ref: ActorRef[ClusterProtocol[T]],
                                   actors: Map[String, ActorRef[T]]
                                 ) extends ClusterProtocol[T]
case class ReceptionistListing[T](listing: Receptionist.Listing) extends ClusterProtocol[T]
case class OrchestratorReady[T]() extends ClusterProtocol[T]
case class OrchestratorDone[T](results: String = null, filename: String = null) extends ClusterProtocol[T]
case class IterationDone[T]() extends ClusterProtocol[T]
