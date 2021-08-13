
package edu.rice.habanero.benchmarks.quicksort

import java.util

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import edu.rice.habanero.actors.AkkaImplicits._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, AkkaMsg, BenchmarkMessage}

import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import gc.Message

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object QuicksortAkkaActorBenchmark {

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

    def runIteration() {

      val system = AkkaActorState.newActorSystem("QuickSort", QuickSortActor(null, null))

      val input = QuickSortConfig.randomlyInitArray()


      AkkaActorState.startActor(system)
      system ! SortMessage(input)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }
  object QuickSortActor {
    def apply(parent: ActorRef[AkkaMsg[QuicksortMsg]], position: Position): Behavior[AkkaMsg[QuicksortMsg]] = {
      Behaviors.setup(context => new QuickSortActor(context, parent, position))
    }
  }
  trait NoRefsMessage extends Message {
    def refs = Seq()
  }
  sealed trait QuicksortMsg extends Message with NoRefsMessage
  abstract class Position
  final case object PositionRight extends Position
  final case object PositionLeft extends Position
  final case object PositionInitial extends Position
  //private abstract class Message
  final case class SortMessage(data: java.util.List[java.lang.Long]) extends QuicksortMsg with NoRefsMessage
  final case class ResultMessage(data: java.util.List[java.lang.Long], position: Position) extends QuicksortMsg with NoRefsMessage

  private class QuickSortActor(context: ActorContext[AkkaMsg[QuicksortMsg]], parent: ActorRef[AkkaMsg[QuicksortMsg]], positionRelativeToParent: Position)
    extends AkkaActor[QuicksortMsg](context) {

    private var result: java.util.List[java.lang.Long] = null
    private var numFragments = 0

    def notifyParentAndTerminate() {

      if (positionRelativeToParent eq PositionInitial) {
        QuickSortConfig.checkSorted(result)
      }
      if (parent ne null) {
        parent ! ResultMessage(result, positionRelativeToParent)
      }
      exit()
    }

    override def process(msg: QuicksortMsg): Behavior[AkkaMsg[QuicksortMsg]] = {
      msg match {
        case SortMessage(data) =>

          val dataLength: Int = data.size()
          if (dataLength < QuickSortConfig.T) {

            result = QuickSortConfig.quicksortSeq(data)
            notifyParentAndTerminate()
            this

          } else {

            val dataLengthHalf = dataLength / 2
            val pivot = data.get(dataLengthHalf)

            val leftUnsorted = QuickSortConfig.filterLessThan(data, pivot)
            //val leftActor = context.system.actorOf(Props(new QuickSortActor(self, PositionLeft)))
            val leftActor = context.spawn(QuickSortActor(context.self, PositionLeft), "Actor_Q1")
            AkkaActorState.startActor(leftActor)
            leftActor ! SortMessage(leftUnsorted)

            val rightUnsorted = QuickSortConfig.filterGreaterThan(data, pivot)
            val rightActor = context.spawn(QuickSortActor(context.self, PositionRight), "Actor_Q2")
            AkkaActorState.startActor(rightActor)
            rightActor ! SortMessage(rightUnsorted)

            result = QuickSortConfig.filterEqualsTo(data, pivot)
            numFragments += 1
            this
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
          this
      }
    }
  }

}
