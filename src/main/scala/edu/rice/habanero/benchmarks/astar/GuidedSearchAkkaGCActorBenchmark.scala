package edu.rice.habanero.benchmarks.astar

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.habanero.benchmarks.astar.GuidedSearchConfig._

import java.util
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ArrayBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object GuidedSearchAkkaGCActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new GuidedSearchAkkaActorBenchmark)
  }

  private final class GuidedSearchAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      GuidedSearchConfig.parseArgs(args)
    }

    def printArgInfo() {
      GuidedSearchConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot(ctx => new Master(latch, ctx)), "GuidedSearch")

      latch.await()

      val nodesProcessed = GuidedSearchConfig.nodesProcessed()
      track("Nodes Processed", nodesProcessed)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
      val valid = GuidedSearchConfig.validate()
      printf(BenchmarkRunner.argOutputFormat, "Result valid", valid)
      GuidedSearchConfig.initializeData()
    }
  }

  trait Msg extends Message
  case class GetMaster(master: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(master)
  }
  case class WorkMessage(node: GuidedSearchConfig.GridNode, target: GuidedSearchConfig.GridNode) extends Msg with NoRefs {
    val priority: Int = GuidedSearchConfig.priority(node)
  }
  case object ReceivedMessage extends Msg with NoRefs
  case object DoneMessage extends Msg with NoRefs
  case object StopMessage extends Msg with NoRefs


  private class Master(latch: CountDownLatch, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private final val numWorkers = GuidedSearchConfig.NUM_WORKERS
    private final val workers = ArrayBuffer.tabulate[ActorRef[Msg]](numWorkers) { i =>
      context.spawnAnonymous(Behaviors.setup(ctx => new Worker(i, ctx)))
    }
    private var numWorkersTerminated: Int = 0
    private var numWorkSent: Int = 0
    private var numWorkCompleted: Int = 0

    var i: Int = 0
    while (i < numWorkers) {
      workers(i) ! GetMaster(context.createRef(context.self, workers(i)))
      i += 1
    }
    sendWork(new WorkMessage(originNode, targetNode))

    private def sendWork(workMessage: WorkMessage) {
      val workerIndex: Int = numWorkSent % numWorkers
      numWorkSent += 1
      workers(workerIndex) ! workMessage
    }

    override def process(theMsg: Msg) {
      theMsg match {
        case workMessage: WorkMessage =>
          sendWork(workMessage)
        case ReceivedMessage =>
          numWorkCompleted += 1
          if (numWorkCompleted == numWorkSent) {
            latch.countDown()
          }
        case DoneMessage =>
          latch.countDown()
        case _ =>
      }
    }
  }

  private class Worker(id: Int, context: ActorContext[Msg]) extends GCActor[Msg](context) {
    private final val threshold = GuidedSearchConfig.THRESHOLD
    private var master: ActorRef[Msg] = _

    override def process(theMsg: Msg) {
      theMsg match {
        case GetMaster(master) =>
          this.master = master
        case workMessage: WorkMessage =>
          search(workMessage)
          master ! ReceivedMessage
        case _ =>
      }
    }

    private def search(workMessage: WorkMessage) {

      val targetNode = workMessage.target
      val workQueue = new util.LinkedList[GridNode]
      workQueue.add(workMessage.node)

      var nodesProcessed: Int = 0
      while (!workQueue.isEmpty && nodesProcessed < threshold) {

        nodesProcessed += 1
        GuidedSearchConfig.busyWait()

        val loopNode = workQueue.poll
        val numNeighbors: Int = loopNode.numNeighbors

        var i: Int = 0
        while (i < numNeighbors) {
          val loopNeighbor = loopNode.neighbor(i)
          val success: Boolean = loopNeighbor.setParent(loopNode)
          if (success) {
            if (loopNeighbor eq targetNode) {
              master ! DoneMessage
              return
            } else {
              workQueue.add(loopNeighbor)
            }
          }
          i += 1
        }
      }

      while (!workQueue.isEmpty) {
        val loopNode = workQueue.poll
        val newWorkMessage = new WorkMessage(loopNode, targetNode)
        master ! newWorkMessage
      }
    }
  }

}
