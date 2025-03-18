package randomworkers

import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import randomworkers.jfr.AppMsgSerialization

import scala.collection.mutable
import java.io.BufferedWriter
import java.io.FileWriter

object Benchmark {

  def remove(ref: ActorRef[Protocol], buf: mutable.ArrayBuffer[ActorRef[Protocol]]): Unit = {
    val i = buf.indexOf(ref)
    if (i != -1)
      buf.remove(i)
  }

  private def isRemote(actor: ActorRef[Nothing])(implicit context: ActorContext[Protocol]): Boolean = {
    actor.path.address != context.system.address
  }

  def sendWorkMsg(recipient : ActorRef[Protocol], work : List[Int])(implicit context: ActorContext[Protocol]) : Unit = {
    recipient ! Work(work)
    if (isRemote(recipient)) {
      val metrics = new AppMsgSerialization()
      metrics.size += recipient.toString.length
      metrics.size = work.size * 4
      metrics.commit()
    }
  }

  def sendAcquaintMsg(recipient : ActorRef[Protocol], workers : Seq[ActorRef[Protocol]])(implicit context: ActorContext[Protocol]) : Unit = {
    recipient ! Acquaint(workers)
    if (isRemote(recipient)) {
      val metrics = new AppMsgSerialization()
      metrics.size += recipient.toString.length
      metrics.size += workers.map(_.toString.length).sum
      metrics.commit()
    }
  }

  // In acyclic mode, we maintain the invariant that owners are always less than their targets.
  def lessThan(owner: ActorRef[Protocol], target: ActorRef[Protocol]): Boolean = {
    owner.path.toString < target.path.toString
  }

  def dumpMeasurements(results: String, filename: String): Unit = {
    if (filename == null) {
      println("ERROR: Missing filename to dump iteration-specific measurements")
    } else {
      println(s"Writing measurements to $filename")
      val writer = new BufferedWriter(new FileWriter(filename, true))
      writer.write(results)
      writer.close()
    }
  }

}
