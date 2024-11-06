package edu.rice.habanero.benchmarks.barber

import java.util.concurrent.atomic.AtomicLong
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer

/**
 * source: https://code.google.com/p/gparallelizer/wiki/ActorsExamples
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SleepingBarberAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SleepingBarberAkkaActorBenchmark)
  }

  private final class SleepingBarberAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SleepingBarberConfig.parseArgs(args)
    }

    def printArgInfo() {
      SleepingBarberConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val idGenerator = new AtomicLong(0)

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot[Msg](ctx => new Master(idGenerator, latch, ctx)), "SleepingBarber")
      latch.await()

      track("CustomerAttempts", idGenerator.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }


  trait Msg extends Message
  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  case object Full extends Msg with NoRefs
  case object Wait extends Msg with NoRefs
  case object Next extends Msg with NoRefs
  case object Start extends Msg with NoRefs
  case object Done extends Msg with NoRefs
  case object Exit extends Msg with NoRefs
  case class Enter(customer: ActorRef[Msg], room: ActorRef[Msg]) extends Msg {
    def refs: Iterable[ActorRef[_]] = List(customer, room)
  }
  case class Returned(customer: ActorRef[Msg]) extends Msg {
    def refs: Iterable[ActorRef[_]] = List(customer)
  }

  private class Master(idGenerator: AtomicLong, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {
      val barber = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new BarberActor(ctx) })
      val room = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new WaitingRoomActor(SleepingBarberConfig.W, barber, ctx) })
      val factoryActor = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new CustomerFactoryActor(idGenerator, SleepingBarberConfig.N, latch, ctx) })
      factoryActor ! Rfmsg(ctx.createRef(room, factoryActor))
      factoryActor ! Start
    }

    override def process(msg: Msg): Unit = ()
  }

  private class WaitingRoomActor(capacity: Int, barber: ActorRef[Msg], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val waitingCustomers = new ListBuffer[ActorRef[Msg]]()
    private var barberAsleep = true

    override def process(msg: Msg) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          if (waitingCustomers.size == capacity) {

            customer ! Full

          } else {

            waitingCustomers.append(customer)
            if (barberAsleep) {

              barberAsleep = false
              ctx.self ! Next

            } else {

              customer ! Wait
            }
          }

        case Next =>

          if (waitingCustomers.nonEmpty) {

            val customer = waitingCustomers.remove(0)
            barber ! new Enter(
              ctx.createRef(customer, barber),
              ctx.createRef(ctx.self, barber)
            )

          } else {

            barber ! Wait
            barberAsleep = true

          }

        case Exit =>

          barber ! Exit
          exit()

      }
    }
  }

  private class BarberActor(ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val random = new PseudoRandom()

    override def process(msg: Msg) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          val room = message.room

          customer ! Start
          // println("Barber: Processing customer " + customer)
          SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.AHR) + 10)
          customer ! Done
          room ! Next

        case Wait =>

        // println("Barber: No customers. Going to have a sleep")

        case Exit =>

          exit()

      }
    }
  }

  private class CustomerFactoryActor(idGenerator: AtomicLong, haircuts: Int,
                                     latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var room: ActorRef[Msg] = _
    private val random = new PseudoRandom()
    private var numHairCutsSoFar = 0

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.room = x
        case Start =>

          var i = 0
          while (i < haircuts) {
            sendCustomerToRoom()
            SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.APR) + 10)
            i += 1
          }

        case message: Returned =>

          idGenerator.incrementAndGet()
          sendCustomerToRoom(message.customer)

        case Done =>

          numHairCutsSoFar += 1
          if (numHairCutsSoFar == haircuts) {
            println("Total attempts: " + idGenerator.get())
            latch.countDown()
          }
      }
    }

    private def sendCustomerToRoom() {
      val customer = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new CustomerActor(idGenerator.incrementAndGet(), ctx) })
      customer ! Rfmsg(ctx.createRef(ctx.self, customer))

      sendCustomerToRoom(customer)
    }

    private def sendCustomerToRoom(customer: ActorRef[Msg]) {
      room ! Enter(
        ctx.createRef(customer, room),
        ctx.createRef(room, room)
      )
    }
  }

  private class CustomerActor(val id: Long, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    var factoryActor: ActorRef[Msg] = _
    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) => this.factoryActor = x
        case Full =>

          // println("Customer-" + id + " The waiting room is full. I am leaving.")
          factoryActor ! new Returned(ctx.createRef(ctx.self, factoryActor))

        case Wait =>

        // println("Customer-" + id + " I will wait.")

        case Start =>

        // println("Customer-" + id + " I am now being served.")

        case Done =>

          //  println("Customer-" + id + " I have been served.")
          factoryActor ! Done
          exit()
      }
    }
  }

}
