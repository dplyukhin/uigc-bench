package edu.rice.habanero.benchmarks.radixsort

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.interfaces.{Message, NoRefs, Refob}
import org.apache.pekko.uigc.{ActorContext, ActorRef, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object RadixSortAkkaGCActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new RadixSortAkkaActorBenchmark)
  }

  private final class RadixSortAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      RadixSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      RadixSortConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot(ctx => new Master(latch, ctx)), "RadixSort")

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  private case class LocalNextActor(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[Refob[Nothing]] = Some(actor)
  }

  private case class NextActorMessage(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[Refob[Nothing]] = Some(actor)
  }

  private case class ValueMessage(value: Long) extends Msg with NoRefs

  private class Master(latch: CountDownLatch, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    val validationActor = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new ValidationActor(RadixSortConfig.N, latch, ctx)))

    val sourceActor = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new IntSourceActor(RadixSortConfig.N, RadixSortConfig.M, RadixSortConfig.S, ctx)))

    var radix = RadixSortConfig.M / 2
    var nextActor: ActorRef[Msg] = validationActor
    while (radix > 0) {

      val localRadix = radix
      val localNextActor = nextActor
      val sortActor = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new SortActor(RadixSortConfig.N, localRadix, ctx)))
      sortActor ! LocalNextActor(context.createRef(localNextActor, sortActor))

      radix /= 2
      context.release(nextActor)
      nextActor = sortActor
    }

    sourceActor ! NextActorMessage(context.createRef(nextActor, sourceActor))
    context.release(sourceActor, nextActor)

    override def process(msg: Msg): Unit = ()
  }

  private class IntSourceActor(numValues: Int, maxValue: Long, seed: Long, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    val random = new PseudoRandom(seed)

    override def process(msg: Msg) {

      msg match {
        case nm: NextActorMessage =>

          var i = 0
          while (i < numValues) {

            val candidate = Math.abs(random.nextLong()) % maxValue
            val message = new ValueMessage(candidate)
            nm.actor ! message

            i += 1
          }

          exit()
      }
    }
  }

  private class SortActor(numValues: Int, radix: Long, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private val orderingArray = Array.ofDim[ValueMessage](numValues)
    private var valuesSoFar = 0
    private var j = 0
    private var nextActor: ActorRef[Msg] = _

    override def process(msg: Msg): Unit = {
      msg match {
        case LocalNextActor(nextActor) =>
          this.nextActor = nextActor
        case vm: ValueMessage =>

          valuesSoFar += 1

          val current = vm.value
          if ((current & radix) == 0) {
            nextActor ! vm
          } else {
            orderingArray(j) = vm
            j += 1
          }

          if (valuesSoFar == numValues) {

            var i = 0
            while (i < j) {
              nextActor ! orderingArray(i)
              i += 1
            }

            exit()
          }
      }
    }
  }

  private class ValidationActor(numValues: Int, latch: CountDownLatch, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private var sumSoFar = 0.0
    private var valuesSoFar = 0
    private var prevValue = 0L
    private var errorValue = (-1L, -1)

    override def process(msg: Msg) {

      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          if (vm.value < prevValue && errorValue._1 < 0) {
            errorValue = (vm.value, valuesSoFar - 1)
          }
          prevValue = vm.value
          sumSoFar += prevValue

          if (valuesSoFar == numValues) {
            if (errorValue._1 >= 0) {
              println("ERROR: Value out of place: " + errorValue._1 + " at index " + errorValue._2)
            } else {
              println("Elements sum: " + sumSoFar)
            }
            latch.countDown()
            exit()
          }
      }
    }
  }

}
