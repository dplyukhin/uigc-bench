package edu.rice.habanero.benchmarks.quicksort

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.interfaces.{Message, NoRefs, Refob}
import org.apache.pekko.uigc.{ActorContext, ActorRef, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util
import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object QuickSortAkkaGCActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new QuickSortAkkaActorBenchmark)
  }

  private final class QuickSortAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      QuickSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      QuickSortConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)

      val input = QuickSortConfig.randomlyInitArray()

      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot(ctx => new QuickSortActor(PositionInitial, latch, ctx)), "QuickSort")
      system ! SortMessage(input, None)

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private abstract class Position

  private case object PositionRight extends Position

  private case object PositionLeft extends Position

  private case object PositionInitial extends Position

  private abstract class Msg extends Message

  private case class SortMessage(data: java.util.List[java.lang.Long], parent: Option[ActorRef[Msg]]) extends Msg {
    override def refs: Iterable[Refob[Nothing]] = parent
  }

  private case class ResultMessage(data: java.util.List[java.lang.Long], position: Position) extends Msg with NoRefs

  private class QuickSortActor(positionRelativeToParent: Position, latch: CountDownLatch, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private var result: java.util.List[java.lang.Long] = null
    private var numFragments = 0
    private var parent: Option[ActorRef[Msg]] = None

    def notifyParentAndTerminate() {

      if (positionRelativeToParent eq PositionInitial) {
        QuickSortConfig.checkSorted(result)
      }
      if (parent.isDefined) {
        parent.get ! ResultMessage(result, positionRelativeToParent)
        context.release(parent)
      }
      else {
        latch.countDown()
      }
      exit()
    }

    override def process(msg: Msg) {
      msg match {
        case SortMessage(data, parent) =>
          this.parent = parent

          val dataLength: Int = data.size()
          if (dataLength < QuickSortConfig.T) {

            result = QuickSortConfig.quicksortSeq(data)
            notifyParentAndTerminate()

          } else {

            val dataLengthHalf = dataLength / 2
            val pivot = data.get(dataLengthHalf)

            val leftUnsorted = QuickSortConfig.filterLessThan(data, pivot)
            val leftActor = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new QuickSortActor(PositionLeft, null, ctx)))
            leftActor ! SortMessage(leftUnsorted, Some(context.createRef(context.self, leftActor)))
            context.release(leftActor)

            val rightUnsorted = QuickSortConfig.filterGreaterThan(data, pivot)
            val rightActor = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new QuickSortActor(PositionRight, null, ctx)))
            rightActor ! SortMessage(rightUnsorted, Some(context.createRef(context.self, rightActor)))
            context.release(rightActor)

            result = QuickSortConfig.filterEqualsTo(data, pivot)
            numFragments += 1
          }

        case ResultMessage(data, position) =>

          if (!data.isEmpty) {
            if (position eq PositionLeft) {
              val temp = new util.ArrayList[java.lang.Long]()
              temp.addAll(data)
              temp.addAll(result)
              result = temp
            } else if (position eq PositionRight) {
              val temp = new util.ArrayList[java.lang.Long]()
              temp.addAll(result)
              temp.addAll(data)
              result = temp
            }
          }

          numFragments += 1
          if (numFragments == 3) {
            notifyParentAndTerminate()
          }
      }
    }
  }

}
