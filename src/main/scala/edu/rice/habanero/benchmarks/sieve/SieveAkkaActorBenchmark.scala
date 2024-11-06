package edu.rice.habanero.benchmarks.sieve

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SieveAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SieveAkkaActorBenchmark)
  }

  private final class SieveAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SieveConfig.parseArgs(args)
    }

    def printArgInfo() {
      SieveConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot[Msg](ctx => new Master(latch, ctx)), "Sieve")
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  trait Msg extends Message
  case class LongBox(value: Long) extends Msg with NoRefs
  case class RefMsg(actorRef: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(actorRef)
  }
  case class StringMsg(value: String) extends Msg with NoRefs


  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {
      val producerActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new NumberProducerActor(SieveConfig.N, ctx) })
      val filterActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new PrimeFilterActor(1, 2, SieveConfig.M, latch, ctx) })
      producerActor ! RefMsg(ctx.createRef(filterActor, producerActor))
    }
    override def process(msg: Msg): Unit = ()
  }

  private class NumberProducerActor(limit: Long, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    override def process(msg: Msg) {
      msg match {
        case RefMsg(filterActor) =>
          var candidate: Long = 3
          while (candidate < limit) {
            filterActor ! LongBox(candidate)
            candidate += 2
          }
          filterActor ! StringMsg("EXIT")
          exit()
      }
    }
  }

  private class PrimeFilterActor(val id: Int, val myInitialPrime: Long, numMaxLocalPrimes: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    var nextFilterActor: ActorRef[Msg] = null
    val localPrimes = new Array[Long](numMaxLocalPrimes)

    var availableLocalPrimes = 1
    localPrimes(0) = myInitialPrime

    private def handleNewPrime(newPrime: Long): Unit = {
      if (SieveConfig.debug)
        println("Found new prime number " + newPrime)
      if (availableLocalPrimes < numMaxLocalPrimes) {
        // Store locally if there is space
        localPrimes(availableLocalPrimes) = newPrime
        availableLocalPrimes += 1
      } else {
        // Create a new actor to store the new prime
        nextFilterActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new PrimeFilterActor(id + 1, newPrime, numMaxLocalPrimes, latch, ctx)})
      }
    }

    override def process(msg: Msg) {
      try {
        msg match {
          case candidate: LongBox =>
            val locallyPrime = SieveConfig.isLocallyPrime(candidate.value, localPrimes, 0, availableLocalPrimes)
            if (locallyPrime) {
              if (nextFilterActor != null) {
                // Pass along the chain to detect for 'primeness'
                nextFilterActor ! candidate
              } else {
                // Found a new prime!
                handleNewPrime(candidate.value)
              }
            }
          case StringMsg(x) =>
            if (nextFilterActor != null) {
              // Signal next actor for termination
              nextFilterActor ! msg
            } else {
              val totalPrimes = ((id - 1) * numMaxLocalPrimes) + availableLocalPrimes
              println("Total primes = " + totalPrimes)
              latch.countDown()
            }
            if (SieveConfig.debug)
              println("Terminating prime actor for number " + myInitialPrime)
            exit()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

}
