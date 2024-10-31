package edu.rice.habanero.benchmarks.count

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.interfaces.{Message, NoRefs, Refob}
import org.apache.pekko.uigc.{ActorContext, ActorRef, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CountingAkkaGCActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CountingAkkaActorBenchmark)
  }

  private final class CountingAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CountingConfig.parseArgs(args)
    }

    def printArgInfo() {
      CountingConfig.printArgs()
    }

    private var system: ActorSystem[Message] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot(ctx => new Root(latch, ctx)), "Counting")


      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class Root(latch: CountDownLatch, context: ActorContext[Message]) extends GCActor[Message](context) {

    val counter = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new CountingActor(ctx)))

    val producer = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new ProducerActor(latch, ctx)))

    producer ! Init(context.createRef(counter, producer))
    producer ! IncrementMessage()
    context.release(counter, producer)

    override def process(msg: Message): Unit = ()
  }

  trait Msg extends Message

  private case class Init(counter: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[Refob[Nothing]] = Some(counter)
  }

  private case class IncrementMessage() extends Msg with NoRefs

  private case class RetrieveMessage(sender: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[Refob[Nothing]] = Some(sender)
  }

  private case class ResultMessage(result: Int) extends Msg with NoRefs

  private class ProducerActor(latch: CountDownLatch, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private var counter: ActorRef[Msg] = _

    override def process(msg: Msg) {
      msg match {
        case m: Init =>
          counter = m.counter

        case m: IncrementMessage =>

          var i = 0
          while (i < CountingConfig.N) {
            counter ! m
            i += 1
          }

          counter ! RetrieveMessage(context.createRef(context.self, counter))
          context.release(counter)

        case m: ResultMessage =>
          val result = m.result
          if (result != CountingConfig.N) {
            println("ERROR: expected: " + CountingConfig.N + ", found: " + result)
          } else {
            println("SUCCESS! received: " + result)
          }
          latch.countDown()
          exit()
      }
    }
  }

  private class CountingActor(context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private var count = 0

    override def process(msg: Msg) {
      msg match {
        case m: IncrementMessage =>
          count += 1
        case m: RetrieveMessage =>
          m.sender ! ResultMessage(count)
          context.release(m.sender)
          exit()
      }
    }
  }

}
