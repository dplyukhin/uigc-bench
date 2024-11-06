package edu.rice.habanero.benchmarks.bitonicsort

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BitonicSortAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BitonicSortAkkaActorBenchmark)
  }

  private final class BitonicSortAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BitonicSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      BitonicSortConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot[Msg](ctx => new Master(latch, ctx)), "BitonicSort")
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  private case class Rfsmsg(actors: Array[ActorRef[Msg]]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = actors
  }
  private case class ValueMessage(value: Long) extends Msg with NoRefs
  private case class DataMessage(orderId: Int, value: Long) extends Msg with NoRefs
  private case class StartMessage() extends Msg with NoRefs
  private case class ExitMessage() extends Msg with NoRefs


  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {
      val validationActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new ValidationActor(BitonicSortConfig.N, latch, ctx) })
      val adapterActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new DataValueAdapterActor(ctx) })
      adapterActor ! Rfmsg(ctx.createRef(validationActor, adapterActor))
      val kernelActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new BitonicSortKernelActor(BitonicSortConfig.N, true, ctx) })
      kernelActor ! Rfmsg(ctx.createRef(adapterActor, kernelActor))
      val sourceActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new IntSourceActor(BitonicSortConfig.N, BitonicSortConfig.M, BitonicSortConfig.S, ctx) })
      sourceActor ! Rfmsg(ctx.createRef(kernelActor, sourceActor))
      sourceActor ! StartMessage()
    }
    override def process(msg: Msg): Unit = ()
  }

  private class ValueDataAdapterActor(orderId: Int, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {
    var nextActor: ActorRef[Msg] = _
    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) => this.nextActor = x
        case vm: ValueMessage =>

          nextActor ! new DataMessage(orderId, vm.value)

        case dm: DataMessage =>

          nextActor ! dm

        case em: ExitMessage =>

          nextActor ! em
          exit()
      }
    }
  }

  private class DataValueAdapterActor(ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    var nextActor: ActorRef[Msg] = _
    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) => this.nextActor = x
        case vm: ValueMessage =>

          nextActor ! vm

        case dm: DataMessage =>

          nextActor ! new ValueMessage(dm.value)

        case em: ExitMessage =>

          nextActor ! em
          exit()
      }
    }
  }

  private class RoundRobinSplitterActor(name: String, length: Int,
                                        ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {

    private var receiverIndex = 0
    private var currentRun = 0
    private var receivers: Array[ActorRef[Msg]] = _

    override def process(msg: Msg) {
      msg match {
        case Rfsmsg(x) =>
          this.receivers = x
        case vm: ValueMessage =>

          receivers(receiverIndex) ! vm
          currentRun += 1
          if (currentRun == length) {
            receiverIndex = (receiverIndex + 1) % receivers.length
            currentRun = 0
          }

        case em: ExitMessage =>

          receivers.foreach(loopActor => loopActor ! em)
          exit()
      }
    }
  }

  private class RoundRobinJoinerActor(name: String, length: Int, numJoiners: Int,
                                      ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val receivedData = Array.tabulate[ListBuffer[DataMessage]](numJoiners)(i => new ListBuffer[DataMessage]())

    private var forwardIndex = 0
    private var currentRun = 0

    private var exitsReceived = 0
    private var nextActor: ActorRef[Msg] = _

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
        case dm: DataMessage =>

          receivedData(dm.orderId).append(dm)
          tryForwardMessages(dm)

        case em: ExitMessage =>

          exitsReceived += 1
          if (exitsReceived == numJoiners) {
            nextActor ! em
            exit()
          }
      }
    }

    def tryForwardMessages(dm: DataMessage) {
      while (receivedData(forwardIndex).nonEmpty) {
        val dm = receivedData(forwardIndex).remove(0)
        val vm = new ValueMessage(dm.value)
        nextActor ! vm
        currentRun += 1
        if (currentRun == length) {
          forwardIndex = (forwardIndex + 1) % numJoiners
          currentRun = 0
        }
      }
    }
  }

  /**
   * Compares the two input keys and exchanges their order if they are not sorted.
   *
   * sortDirection determines if the sort is nondecreasing (UP) [true] or nonincreasing (DOWN) [false].
   */
  private class CompareExchangeActor(orderId: Int, sortDirection: Boolean, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {
    private var nextActor: ActorRef[Msg] = _
    private var k1: Long = 0
    private var valueAvailable = false

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
        case vm: ValueMessage =>

          if (!valueAvailable) {

            valueAvailable = true
            k1 = vm.value

          } else {

            valueAvailable = false
            val k2 = vm.value
            val (minK, maxK) = if (k1 <= k2) (k1, k2) else (k2, k1)
            if (sortDirection) {
              // UP sort
              nextActor ! DataMessage(orderId, minK)
              nextActor ! DataMessage(orderId, maxK)
            } else {
              // DOWN sort
              nextActor ! DataMessage(orderId, maxK)
              nextActor ! DataMessage(orderId, minK)
            }

          }

        case em: ExitMessage =>

          nextActor ! em
          exit()
      }
    }
  }

  /**
   * Partition the input bitonic sequence of length L into two bitonic sequences of length L/2,
   * with all numbers in the first sequence <= all numbers in the second sequence if sortdir is UP (similar case for DOWN sortdir)
   *
   * Graphically, it is a bunch of CompareExchanges with same sortdir, clustered together in the sort network at a particular step (of some merge stage).
   */
  private class PartitionBitonicSequenceActor(orderId: Int, length: Int, sortDir: Boolean, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {
    private var nextActor: ActorRef[Msg] = _
    val halfLength = length / 2
    var forwardActor: ActorRef[Msg] = _
    var joinerActor: ActorRef[Msg] = _
    var splitterActor: ActorRef[Msg] = _
    var workerActors: Array[ActorRef[Msg]] = _


    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
          forwardActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new ValueDataAdapterActor(orderId, ctx)})
            actor ! Rfmsg(ctx.createRef(nextActor, actor))
            actor
          }
          joinerActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new RoundRobinJoinerActor("Partition-" + orderId, 1, halfLength, ctx)})
            actor ! Rfmsg(ctx.createRef(forwardActor, actor))
            actor
          }
          workerActors = Array.tabulate[ActorRef[Msg]](halfLength)(i => {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new CompareExchangeActor(i, sortDir, ctx)})
            actor ! Rfmsg(ctx.createRef(joinerActor, actor))
            actor
          })
          splitterActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new RoundRobinSplitterActor("Partition-" + orderId, 1, ctx)})
            val workerRefs = workerActors.map(ctx.createRef(_, actor))
            actor ! Rfsmsg(workerRefs)
            actor
          }
        case vm: ValueMessage =>

          splitterActor ! vm

        case em: ExitMessage =>

          splitterActor ! em
          exit()
      }
    }
  }

  /**
   * One step of a particular merge stage (used by all merge stages except the last)
   *
   * directionCounter determines which step we are in the current merge stage (which in turn is determined by <L, numSeqPartitions>)
   */
  private class StepOfMergeActor(orderId: Int, length: Int, numSeqPartitions: Int, directionCounter: Int,
                                 ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    var forwardActor: ActorRef[Msg] = _
    var joinerActor: ActorRef[Msg] = _
    var splitterActor: ActorRef[Msg] = _
    var workerActors: Array[ActorRef[Msg]] = _


    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x

          forwardActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new DataValueAdapterActor(ctx)})
            actor ! Rfmsg(ctx.createRef(nextActor, actor))
            actor
          }
          joinerActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new RoundRobinJoinerActor("StepOfMerge-" + orderId + ":" + length, length, numSeqPartitions, ctx)})
            actor ! Rfmsg(ctx.createRef(forwardActor, actor))
            actor
          }
          workerActors = Array.tabulate[ActorRef[Msg]](numSeqPartitions)(i => {
            // finding out the currentDirection is a bit tricky -
            // the direction depends only on the subsequence number during the FIRST step.
            // So to determine the FIRST step subsequence to which this sequence belongs, divide this sequence's number j by directionCounter
            // (bcoz 'directionCounter' tells how many subsequences of the current step make up one subsequence of the FIRST step).
            // Then, test if that result is even or odd to determine if currentDirection is UP or DOWN respectively.
            val currentDirection = (i / directionCounter) % 2 == 0

            // The last step needs special care to avoid split-joins with just one branch.
            if (length > 2) {
              val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new PartitionBitonicSequenceActor(i, length, currentDirection, ctx)})
              actor ! Rfmsg(ctx.createRef(joinerActor, actor))
              actor
            } else {
              // PartitionBitonicSequence of the last step (L=2) is simply a CompareExchange
              val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new CompareExchangeActor(i, currentDirection, ctx)})
              actor ! Rfmsg(ctx.createRef(joinerActor, actor))
              actor
            }
          })
          splitterActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new RoundRobinSplitterActor("StepOfMerge-" + orderId + ":" + length, length, ctx)})
            val workerRefs = workerActors.map(ctx.createRef(_, actor))
            actor ! Rfsmsg(workerRefs)
            actor
          }
        case vm: ValueMessage =>

          splitterActor ! vm

        case em: ExitMessage =>

          splitterActor ! em
          exit()
      }
    }
  }

  /**
   * One step of the last merge stage
   *
   * Main difference form StepOfMerge is the direction of sort.
   * It is always in the same direction - sortdir.
   */
  private class StepOfLastMergeActor(length: Int, numSeqPartitions: Int, sortDirection: Boolean, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {

    var nextActor: ActorRef[Msg] = _
    var joinerActor: ActorRef[Msg] = _
    var splitterActor: ActorRef[Msg] = _
    var workerActors: Array[ActorRef[Msg]] = _

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
          joinerActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new RoundRobinJoinerActor("StepOfLastMerge-" + length, length, numSeqPartitions, ctx)})
            actor ! Rfmsg(ctx.createRef(nextActor, actor))
            actor
          }
          workerActors = Array.tabulate[ActorRef[Msg]](numSeqPartitions)(i => {
            // The last step needs special care to avoid split-joins with just one branch.
            if (length > 2) {
              val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new PartitionBitonicSequenceActor(i, length, sortDirection, ctx)})
              actor ! Rfmsg(ctx.createRef(joinerActor, actor))
              actor
            } else {
              // PartitionBitonicSequence of the last step (L=2) is simply a CompareExchange
              val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new CompareExchangeActor(i, sortDirection, ctx)})
              actor ! Rfmsg(ctx.createRef(joinerActor, actor))
              actor
            }
          })
          splitterActor = {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new RoundRobinSplitterActor("StepOfLastMerge-" + length, length, ctx)})
            val workerRefs = workerActors.map(ctx.createRef(_, actor))
            actor ! Rfsmsg(workerRefs)
            actor
          }
        case vm: ValueMessage =>

          splitterActor ! vm

        case em: ExitMessage =>

          splitterActor ! em
          exit()
      }
    }
  }

  /**
   * Divide the input sequence of length N into subsequences of length P and sort each of them
   * (either UP or DOWN depending on what subsequence number [0 to N/P-1] they get.
   * All even subsequences are sorted UP and all odd subsequences are sorted DOWN).
   * In short, a MergeStage is N/P Bitonic Sorters of order P each.
   * But, this MergeStage is implemented *iteratively* as logP STEPS.
   */
  private class MergeStageActor(P: Int, N: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    private var nextActor: ActorRef[Msg] = _
    var forwardActor: ActorRef[Msg] = _

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
          this.forwardActor = {
            var loopActor: ActorRef[Msg] = nextActor

            // for each of the lopP steps (except the last step) of this merge stage
            var i = P / 2
            while (i >= 1) {

              // length of each sequence for the current step - goes like P, P/2, ..., 2.
              val L = P / i
              // numSeqPartitions is the number of PartitionBitonicSequence-rs in this step
              val numSeqPartitions = (N / P) * i
              val directionCounter = i

              val localLoopActor = loopActor
              val tempActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new StepOfMergeActor(i, L, numSeqPartitions, directionCounter, ctx)})
              tempActor ! Rfmsg(ctx.createRef(localLoopActor, tempActor))
              loopActor = tempActor

              i /= 2
            }

            loopActor
          }
        case vm: ValueMessage =>

          forwardActor ! vm

        case em: ExitMessage =>

          forwardActor ! em
          exit()
      }
    }
  }

  /**
   * The LastMergeStage is basically one Bitonic Sorter of order N i.e.,
   * it takes the bitonic sequence produced by the previous merge stages
   * and applies a bitonic merge on it to produce the final sorted sequence.
   *
   * This is implemented iteratively as logN steps.
   */
  private class LastMergeStageActor(N: Int, sortDirection: Boolean, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    private var nextActor: ActorRef[Msg] = _

    private var forwardActor: ActorRef[Msg] = _

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
          this.forwardActor = {
              var loopActor: ActorRef[Msg] = nextActor

              // for each of the lopN steps (except the last step) of this merge stage
              var i = N / 2
              while (i >= 1) {

                // length of each sequence for the current step - goes like N, N/2, ..., 2.
                val L = N / i
                // numSeqPartitions is the number of PartitionBitonicSequence-rs in this step
                val numSeqPartitions = i

                val localLoopActor = loopActor
                val tempActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new StepOfLastMergeActor(L, numSeqPartitions, sortDirection, ctx)})
                tempActor ! Rfmsg(ctx.createRef(localLoopActor, tempActor))
                loopActor = tempActor

                i /= 2
              }

              loopActor
            }
        case vm: ValueMessage =>

          forwardActor ! vm

        case em: ExitMessage =>

          forwardActor ! em
          exit()
      }
    }
  }

  /**
   * The top-level kernel of bitonic-sort (iterative version) -
   * It has logN merge stages and all merge stages except the last progressively builds a bitonic sequence out of the input sequence.
   * The last merge stage acts on the resultant bitonic sequence to produce the final sorted sequence (sortdir determines if it is UP or DOWN).
   */
  private class BitonicSortKernelActor(N: Int, sortDirection: Boolean, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    private var nextActor: ActorRef[Msg] = _
    private var forwardActor: ActorRef[Msg] = _

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
          this.forwardActor = {
            var loopActor: ActorRef[Msg] = nextActor

            {
              val localLoopActor = loopActor
              val tempActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new LastMergeStageActor(N, sortDirection, ctx)})
              tempActor ! Rfmsg(ctx.createRef(localLoopActor, tempActor))
              loopActor = tempActor
            }

            var i = N / 2
            while (i >= 2) {

              val localLoopActor = loopActor
              val tempActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new MergeStageActor(i, N, ctx)})
              tempActor ! Rfmsg( ctx.createRef(localLoopActor, tempActor))
              loopActor = tempActor

              i /= 2
            }

            loopActor
          }
        case vm: ValueMessage =>

          forwardActor ! vm

        case em: ExitMessage =>

          forwardActor ! em
          exit()
      }
    }
  }

  private class IntSourceActor(numValues: Int, maxValue: Long, seed: Long, ctx: ActorContext[Msg])
    extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    private val random = new PseudoRandom(seed)
    private val sb = new StringBuilder()

    override def process(msg: Msg) {

      msg match {
        case Rfmsg(x) =>
          this.nextActor = x
        case nm: StartMessage =>

          var i = 0
          while (i < numValues) {

            val candidate = Math.abs(random.nextLong()) % maxValue
            if (BitonicSortConfig.debug) {
              sb.append(candidate + " ")
            }
            val message = new ValueMessage(candidate)
            nextActor ! message

            i += 1
          }
          if (BitonicSortConfig.debug) {
            println("  SOURCE: " + sb)
          }

          nextActor ! ExitMessage()
          exit()
      }
    }
  }

  private class ValidationActor(numValues: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var sumSoFar = 0.0
    private var valuesSoFar = 0
    private var prevValue = 0L
    private var errorValue = (-1L, -1)
    private val sb = new StringBuilder()

    override def process(msg: Msg) {

      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          if (BitonicSortConfig.debug) {
            sb.append(vm.value + " ")
          }
          if (vm.value < prevValue && errorValue._1 < 0) {
            errorValue = (vm.value, valuesSoFar - 1)
          }
          prevValue = vm.value
          sumSoFar += prevValue

        case em: ExitMessage =>

          if (valuesSoFar == numValues) {
            if (BitonicSortConfig.debug) {
              println("  OUTPUT: " + sb)
            }
            if (errorValue._1 >= 0) {
              println("  ERROR: Value out of place: " + errorValue._1 + " at index " + errorValue._2)
            } else {
              println("  Elements sum: " + sumSoFar)
            }
          } else {
            println("  ERROR: early exit triggered, received only " + valuesSoFar + " values!")
          }

          latch.countDown()
      }
    }
  }

}
