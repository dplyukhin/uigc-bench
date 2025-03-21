package edu.rice.habanero.benchmarks.fib

import edu.rice.habanero.actors.{AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciAkkaGCActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FibonacciAkkaActorBenchmark)
  }

  private final class FibonacciAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FibonacciConfig.parseArgs(args)
    }

    def printArgInfo() {
      FibonacciConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _

    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot(ctx => new FibonacciActor(latch, ctx)), "Fibonacci")

      system ! Request(FibonacciConfig.N, None)

      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  private case class Request(n: Int, parent: Option[ActorRef[Msg]]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = parent
  }
  private case class Response(value: Int) extends Msg with NoRefs

  private val RESPONSE_ONE = Response(1)

  private class FibonacciActor(latch: CountDownLatch, context: ActorContext[Msg]) extends GCActor[Msg](context) {

    private var result = 0
    private var respReceived = 0
    private var parent: Option[ActorRef[Msg]] = None

    override def process(msg: Msg) {

      msg match {
        case req: Request =>
          parent = req.parent

          if (req.n <= 2) {

            result = 1
            processResult(RESPONSE_ONE)

          } else {

            val f1 = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new FibonacciActor(null, ctx)))
            f1 ! Request(req.n - 1, Some(context.createRef(context.self, f1)))

            val f2 = context.spawnAnonymous(Behaviors.setup[Msg](ctx => new FibonacciActor(null, ctx)))
            f2 ! Request(req.n - 2, Some(context.createRef(context.self, f2)))

            //context.release(f1, f2)

          }

        case resp: Response =>

          respReceived += 1
          result += resp.value

          if (respReceived == 2) {
            processResult(Response(result))
          }
      }
    }

    private def processResult(response: Response) {
      if (parent.isDefined) {
        parent.get ! response
        //context.release(parent.get)
      } else {
        latch.countDown()
        println(" Result = " + result)
      }
    }
  }

}
