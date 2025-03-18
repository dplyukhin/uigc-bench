package randomworkers

import org.apache.pekko.uigc.actor.typed.{ActorRef, Message, NoRefs}

import scala.collection.mutable

trait Protocol extends Serializable with Message

case class LearnPeers(peers: mutable.ArrayBuffer[ActorRef[Protocol]]) extends Protocol {
  override def refs: Iterable[ActorRef[Nothing]] = peers
}

case class Work(work: List[Int]) extends Protocol with NoRefs

case class Acquaint(workers: Seq[ActorRef[Protocol]]) extends Protocol {
  override def refs: Iterable[ActorRef[Nothing]] = workers
}

case class Query(n: Int, master: ActorRef[Protocol]) extends Protocol {
  override def refs: Iterable[ActorRef[Nothing]] = Some(master)
}

case class QueryResponse(n: Int) extends Protocol with NoRefs

case object Ping extends Protocol with NoRefs

case object TriggerGC extends Protocol with NoRefs
