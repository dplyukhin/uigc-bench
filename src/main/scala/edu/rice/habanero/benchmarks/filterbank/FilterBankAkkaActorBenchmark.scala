package edu.rice.habanero.benchmarks.filterbank

import java.util
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.Collection
import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FilterBankAkkaActorBenchmark {

  def main(args: Array[String]) {
    println("WARNING: AKKA version DEADLOCKS. Needs debugging!")
    BenchmarkRunner.runBenchmark(args, new FilterBankAkkaActorBenchmark)
  }

  private final class FilterBankAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FilterBankConfig.parseArgs(args)
    }

    def printArgInfo() {
      FilterBankConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {
      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot[Msg](ctx => new Master(latch, ctx)), "FilterBank")
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      AkkaActorState.awaitTermination(system)
    }
  }


  private trait Msg extends Message
  private case class ProducerNextActorMsg(producer: ActorRef[Msg], nextActor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(producer, nextActor)
  }
  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  private case class NextMessage(source: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(source)
  }
  private case object BootMessage extends Msg with NoRefs
  private case object ExitMessage extends Msg with NoRefs
  private case class ValueMessage(value: Double) extends Msg with NoRefs
  private case class SourcedValueMessage(sourceId: Int, value: Double) extends Msg with NoRefs
  private case class CollectionMessage[T](values: util.Collection[T]) extends Msg with NoRefs


  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {
      val numSimulations: Int = FilterBankConfig.NUM_SIMULATIONS
      val numChannels: Int = FilterBankConfig.NUM_CHANNELS
      val numColumns: Int = FilterBankConfig.NUM_COLUMNS
      val H: Array[Array[Double]] = FilterBankConfig.H
      val F: Array[Array[Double]] = FilterBankConfig.F
      val sinkPrintRate: Int = FilterBankConfig.SINK_PRINT_RATE

      // create the pipeline of actors
      val producer = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new ProducerActor(numSimulations, ctx) })
      val sink = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SinkActor(sinkPrintRate, ctx) })
      val combine = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new CombineActor(ctx) })
      combine ! Rfmsg(ctx.createRef(sink, combine))
      val integrator = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new IntegratorActor(numChannels, ctx) })
      integrator ! Rfmsg(ctx.createRef(combine, integrator))
      val branches = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new BranchesActor(numChannels, numColumns, H, F, ctx) })
      branches ! Rfmsg(ctx.createRef(integrator, branches))
      val source = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SourceActor(latch, ctx) })
      source ! ProducerNextActorMsg(ctx.createRef(producer, source), ctx.createRef(branches, source))

      // start the pipeline
      producer ! new NextMessage(ctx.createRef(source, producer))
    }
    override def process(msg: Msg): Unit = ()
  }

  private class ProducerActor(numSimulations: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var numMessagesSent: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case message: NextMessage =>
          val source = message.source
          if (numMessagesSent == numSimulations) {
            source ! ExitMessage
            exit()
          }
          else {
            source ! BootMessage
            numMessagesSent += 1
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SourceActor(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var producer: ActorRef[Msg] = _
    private var nextActor: ActorRef[Msg] = _
    private final val maxValue: Int = 1000
    private var current: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case ProducerNextActorMsg(producer, nextActor) =>
          this.producer = producer
          this.nextActor = nextActor
        case BootMessage =>
          nextActor ! new ValueMessage(current)
          current = (current + 1) % maxValue
          producer ! new NextMessage(ctx.createRef(ctx.self, producer))
        case ExitMessage =>
          latch.countDown()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }

    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

  private class SinkActor(printRate: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var count: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case message: ValueMessage =>
          val result: Double = message.value
          if (FilterBankConfig.debug && (count == 0)) {
            System.out.println("SinkActor: result = " + result)
          }
          count = (count + 1) % printRate
        case ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class BranchesActor(numChannels: Int, numColumns: Int, H: Array[Array[Double]], F: Array[Array[Double]], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    private var banks: Array[ActorRef[Msg]] = _

    protected override def onPostExit() {
      for (loopBank <- banks) {
        loopBank ! ExitMessage
      }
    }

    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          nextActor = x
          banks = Array.tabulate[ActorRef[Msg]](numChannels)(i => {
            val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new BankActor(i, numColumns, H(i), F(i), ctx)})
            actor ! Rfmsg(ctx.createRef(nextActor, actor))
            actor
          })
        case _: ValueMessage =>
          for (loopBank <- banks) {
            loopBank ! theMsg
          }
        case ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class BankActor(sourceId: Int, numColumns: Int, H: Array[Double], F: Array[Double], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var integrator: ActorRef[Msg] = _
    private var firstActor: ActorRef[Msg] = _

    protected override def onPostExit() {
      firstActor ! ExitMessage
    }

    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          integrator = x

          firstActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new DelayActor(sourceId + ".1", numColumns - 1, ctx)})
          val secondActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new FirFilterActor(sourceId + ".1", numColumns, H, ctx)})
          firstActor ! Rfmsg(ctx.createRef(secondActor, firstActor))
          val thirdActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SampleFilterActor(numColumns, ctx)})
          secondActor ! Rfmsg(ctx.createRef(thirdActor, secondActor))
          val fourthActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new DelayActor(sourceId + ".2", numColumns - 1, ctx)})
          thirdActor ! Rfmsg(ctx.createRef(fourthActor, thirdActor))
          val fifthActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new FirFilterActor(sourceId + ".2", numColumns, F, ctx)})
          fourthActor ! Rfmsg(ctx.createRef(fifthActor, fourthActor))
          val sixthActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new TaggedForwardActor(sourceId, ctx)})
          sixthActor ! Rfmsg(ctx.createRef(integrator, sixthActor))
        case _: ValueMessage =>
          firstActor ! theMsg
        case ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class DelayActor(sourceId: String, delayLength: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    private final val state = Array.tabulate[Double](delayLength)(i => 0)
    private var placeHolder: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          nextActor = x
        case message: ValueMessage =>
          val result: Double = message.value
          nextActor ! new ValueMessage(state(placeHolder))
          state(placeHolder) = result
          placeHolder = (placeHolder + 1) % delayLength
        case ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

  private class FirFilterActor(sourceId: String, peekLength: Int, coefficients: Array[Double], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    private var data = Array.tabulate[Double](peekLength)(i => 0)
    private var dataIndex: Int = 0
    private var dataFull: Boolean = false

    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          nextActor = x
        case message: ValueMessage =>
          val result: Double = message.value
          data(dataIndex) = result
          dataIndex += 1
          if (dataIndex == peekLength) {
            dataFull = true
            dataIndex = 0
          }
          if (dataFull) {
            var sum: Double = 0.0
            var i: Int = 0
            while (i < peekLength) {
              sum += (data(i) * coefficients(peekLength - i - 1))
              i += 1
            }
            nextActor ! new ValueMessage(sum)
          }
        case ExitMessage =>
          exit()
        case _ =>
      }
    }
    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

  private object SampleFilterActor {
    private final val ZERO_RESULT: ValueMessage = ValueMessage(0)
  }

  private class SampleFilterActor(sampleRate: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    private var samplesReceived: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          nextActor = x
        case _: ValueMessage =>
          if (samplesReceived == 0) {
            nextActor ! theMsg
          } else {
            nextActor ! SampleFilterActor.ZERO_RESULT
          }
          samplesReceived = (samplesReceived + 1) % sampleRate
        case ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

  private class TaggedForwardActor(sourceId: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          nextActor = x
        case message: ValueMessage =>
          val result: Double = message.value
          nextActor ! new SourcedValueMessage(sourceId, result)
        case ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

  private class IntegratorActor(numChannels: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    private final val data = new java.util.ArrayList[util.Map[Integer, Double]]
    private var exitsReceived: Int = 0

    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          nextActor = x
        case message: SourcedValueMessage =>
          val sourceId: Int = message.sourceId
          val result: Double = message.value
          val dataSize: Int = data.size
          var processed: Boolean = false
          var i: Int = 0
          while (i < dataSize) {
            val loopMap: java.util.Map[Integer, Double] = data.get(i)
            if (!loopMap.containsKey(sourceId)) {
              loopMap.put(sourceId, result)
              processed = true
              i = dataSize
            }
            i += 1
          }
          if (!processed) {
            val newMap = new java.util.HashMap[Integer, Double]
            newMap.put(sourceId, result)
            data.add(newMap)
          }
          val firstMap: java.util.Map[Integer, Double] = data.get(0)
          if (firstMap.size == numChannels) {
            nextActor ! new CollectionMessage[Double](firstMap.values)
            data.remove(0)
          }
        case ExitMessage =>
          exitsReceived += 1
          if (exitsReceived == numChannels) {
            exit()
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

  private class CombineActor(ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var nextActor: ActorRef[Msg] = _
    override def process(theMsg: Msg) {
      theMsg match {
        case Rfmsg(x) =>
          nextActor = x
        case message: CollectionMessage[_] =>
          import scala.jdk.CollectionConverters._
          val result = message.values.asInstanceOf[util.Collection[Double]].asScala
          var sum: Double = 0
          for (loopValue <- result) {
            sum += loopValue
          }
          nextActor ! new ValueMessage(sum)
        case ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
    protected override def onPostExit() {
      nextActor ! ExitMessage
    }
  }

}
