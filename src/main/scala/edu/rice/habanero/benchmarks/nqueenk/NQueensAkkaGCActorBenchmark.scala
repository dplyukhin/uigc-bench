package edu.rice.habanero.benchmarks.nqueenk

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.interfaces.{Message, NoRefs, Refob}
import org.apache.pekko.uigc.{ActorContext, ActorRef, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ArrayBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object NQueensAkkaGCActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new NQueensAkkaActorBenchmark)
  }

  private final class NQueensAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      NQueensConfig.parseArgs(args)
    }

    def printArgInfo() {
      NQueensConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {
      val numWorkers: Int = NQueensConfig.NUM_WORKERS
      val priorities: Int = NQueensConfig.PRIORITIES

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot[Msg](ctx => new Master(numWorkers, priorities, latch, ctx)), "NQueens")

      latch.await()

      val expSolution = NQueensConfig.SOLUTIONS(NQueensConfig.SIZE - 1)
      val actSolution = Master.resultCounter
      val solutionsLimit = NQueensConfig.SOLUTIONS_LIMIT
      val valid = actSolution >= solutionsLimit && actSolution <= expSolution
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
      Master.resultCounter = 0
    }
  }

  trait Msg extends Message
  case class MasterMsg(master: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[Refob[Nothing]] = Some(master)
  }
  case class WorkMessage(_priority: Int, data: Array[Int], depth: Int) extends Msg with NoRefs {
    val priority = Math.min(NQueensConfig.PRIORITIES - 1, Math.max(0, _priority))
  }
  case object ResultMessage extends Msg with NoRefs
  case object DoneMessage extends Msg with NoRefs
  case object StopMessage extends Msg with NoRefs

  object Master {
    var resultCounter: Long = 0
  }

  private class Master(numWorkers: Int, priorities: Int, latch: CountDownLatch, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private val solutionsLimit = NQueensConfig.SOLUTIONS_LIMIT
    private var messageCounter: Int = 0
    private var numWorkersTerminated: Int = 0
    private var numWorkSent: Int = 0
    private var numWorkCompleted: Int = 0

    private final val workers = ArrayBuffer.tabulate[ActorRef[Msg]](numWorkers) { i =>
      context.spawnAnonymous(Behaviors.setup[Msg](ctx => new Worker(i, ctx)))
    }
    for (worker <- workers)
      worker ! MasterMsg(context.createRef(context.self, worker))
    val inArray: Array[Int] = new Array[Int](0)
    val workMessage = WorkMessage(priorities, inArray, 0)
    sendWork(workMessage)

    private def sendWork(workMessage: WorkMessage) {
      workers(messageCounter) ! workMessage
      messageCounter = (messageCounter + 1) % numWorkers
      numWorkSent += 1
    }

    override def process(theMsg: Msg) {
      theMsg match {
        case workMessage: WorkMessage =>
          sendWork(workMessage)
        case ResultMessage =>
          Master.resultCounter += 1
          if (Master.resultCounter == solutionsLimit) {
            latch.countDown()
          }
        case DoneMessage =>
          numWorkCompleted += 1
          if (numWorkCompleted == numWorkSent) {
            latch.countDown()
          }
        case StopMessage =>
          numWorkersTerminated += 1
          if (numWorkersTerminated == numWorkers) {
            exit()
          }
        case _ =>
      }
    }

    def requestWorkersToTerminate() {
      var i: Int = 0
      while (i < numWorkers) {
        workers(i) ! StopMessage
        i += 1
      }
    }
  }

  private class Worker(id: Int, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private final val threshold: Int = NQueensConfig.THRESHOLD
    private final val size: Int = NQueensConfig.SIZE
    private var master: ActorRef[Msg] = _

    override def process(theMsg: Msg) {
      theMsg match {
        case MasterMsg(master) =>
          this.master = master
        case workMessage: WorkMessage =>
          nqueensKernelPar(workMessage)
          master ! DoneMessage
        case StopMessage =>
          master ! theMsg
          context.release(master)
          exit()
        case _ =>
      }
    }

    private[nqueenk] def nqueensKernelPar(workMessage: WorkMessage) {
      val a: Array[Int] = workMessage.data
      val depth: Int = workMessage.depth
      if (size == depth) {
        master ! ResultMessage
      } else if (depth >= threshold) {
        nqueensKernelSeq(a, depth)
      } else {
        val newPriority: Int = workMessage.priority - 1
        val newDepth: Int = depth + 1
        var i: Int = 0
        while (i < size) {
          val b: Array[Int] = new Array[Int](newDepth)
          System.arraycopy(a, 0, b, 0, depth)
          b(depth) = i
          if (NQueensConfig.boardValid(newDepth, b)) {
            master ! WorkMessage(newPriority, b, newDepth)
          }
          i += 1
        }
      }
    }

    private[nqueenk] def nqueensKernelSeq(a: Array[Int], depth: Int) {
      if (size == depth) {
        master ! ResultMessage
      }
      else {
        val b: Array[Int] = new Array[Int](depth + 1)

        var i: Int = 0
        while (i < size) {
          System.arraycopy(a, 0, b, 0, depth)
          b(depth) = i
          if (NQueensConfig.boardValid(depth + 1, b)) {
            nqueensKernelSeq(b, depth + 1)
          }

          i += 1
        }
      }
    }
  }

}
