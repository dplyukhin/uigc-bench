package edu.rice.habanero.benchmarks.bndbuffer

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.bndbuffer.ProdConsBoundedBufferConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ProdConsAkkaActorBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ProdConsAkkaActorBenchmark)
  }

  private final class ProdConsAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ProdConsBoundedBufferConfig.parseArgs(args)
    }

    def printArgInfo() {
      ProdConsBoundedBufferConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new ManagerActor(
            ProdConsBoundedBufferConfig.bufferSize,
            ProdConsBoundedBufferConfig.numProducers,
            ProdConsBoundedBufferConfig.numConsumers,
            ProdConsBoundedBufferConfig.numItemsPerProducer,
            latch,
            ctx)
        ),
        "ProdCons")
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }

    private trait Msg extends Message
    private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
      override def refs: Iterable[ActorRef[_]] = Some(actor)
    }
    private case class DataItemMessage(data: Double, producer: ActorRef[Msg]) extends Msg {
      override def refs: Iterable[ActorRef[_]] = Some(producer)
    }
    private case object ProduceDataMessage extends Msg with NoRefs
    private case object ProducerExitMessage extends Msg with NoRefs
    private case class ConsumerAvailableMessage(consumer: ActorRef[Msg]) extends Msg {
      override def refs: Iterable[ActorRef[_]] = Some(consumer)
    }
    private case object ConsumerExitMessage extends Msg with NoRefs


    private class ManagerActor(bufferSize: Int, numProducers: Int, numConsumers: Int, numItemsPerProducer: Int,
                               latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {


      private val adjustedBufferSize: Int = bufferSize - numProducers
      private val availableProducers = new ListBuffer[ActorRef[Msg]]
      private val availableConsumers = new ListBuffer[ActorRef[Msg]]
      private val pendingData = new ListBuffer[DataItemMessage]
      private var numTerminatedProducers: Int = 0

      private val producers = Array.tabulate[ActorRef[Msg]](numProducers)(i => {
        val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new ProducerActor(i, numItemsPerProducer, ctx)})
        actor ! Rfmsg(ctx.createRef(ctx.self, actor))
        actor
      })
      private val consumers = Array.tabulate[ActorRef[Msg]](numConsumers)(i => {
        val actor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new ConsumerActor(i, ctx)})
        actor ! Rfmsg(ctx.createRef(ctx.self, actor))
        actor
      }
      )

      consumers.foreach(loopConsumer => {
        availableConsumers.append(loopConsumer)
      })

      producers.foreach(loopProducer => {
        loopProducer ! ProduceDataMessage
      })

      override def onPreExit() {
        consumers.foreach(loopConsumer => {
          loopConsumer ! ConsumerExitMessage
        })
      }

      override def process(theMsg: Msg) {
        theMsg match {
          case dm: DataItemMessage =>
            val producer: ActorRef[Msg] = dm.producer
            if (availableConsumers.isEmpty) {
              pendingData.append(dm)
            } else {
              val consumer = availableConsumers.remove(0)
              consumer ! DataItemMessage(dm.data, ctx.createRef(producer, consumer))
            }
            if (pendingData.size >= adjustedBufferSize) {
              availableProducers.append(producer)
            } else {
              producer ! ProduceDataMessage
            }
          case cm: ConsumerAvailableMessage =>
            val consumer: ActorRef[Msg] = cm.consumer
            if (pendingData.isEmpty) {
              availableConsumers.append(consumer)
              tryExit()
            } else {
              consumer ! pendingData.remove(0)
              if (!availableProducers.isEmpty) {
                availableProducers.remove(0) ! ProduceDataMessage
              }
            }
          case ProducerExitMessage =>
            numTerminatedProducers += 1
            tryExit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }

      def tryExit() {
        if (numTerminatedProducers == numProducers && availableConsumers.size == numConsumers) {
          latch.countDown()
        }
      }
    }

    private class ProducerActor(id: Int, numItemsToProduce: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

      private var manager: ActorRef[Msg] = _
      private var prodItem: Double = 0.0
      private var itemsProduced: Int = 0

      private def produceData() {
        prodItem = processItem(prodItem, prodCost)
        manager ! new DataItemMessage(prodItem, ctx.createRef(ctx.self, manager))
        itemsProduced += 1
      }

      override def process(theMsg: Msg) {
        theMsg match {
          case Rfmsg(x) =>
            this.manager = x
          case ProduceDataMessage =>
            if (itemsProduced == numItemsToProduce) {
              exit()
            } else {
              produceData()
            }
        }
      }

      override def onPreExit() {
        manager ! ProducerExitMessage
      }
    }

    private class ConsumerActor(id: Int, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

      private var manager: ActorRef[Msg] = _
      private var consItem: Double = 0

      protected def consumeDataItem(dataToConsume: Double) {
        consItem = processItem(consItem + dataToConsume, consCost)
      }

      override def process(theMsg: Msg) {
        theMsg match {
          case Rfmsg(x) => this.manager = x
          case dm: DataItemMessage =>
            consumeDataItem(dm.data)
            manager ! ConsumerAvailableMessage(ctx.createRef(ctx.self, manager))
          case ConsumerExitMessage =>
            exit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }
    }

  }

}
