package edu.rice.habanero.benchmarks.banking

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BankingAkkaManualStashActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BankingAkkaManualStashActorBenchmark)
  }

  private final class BankingAkkaManualStashActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BankingConfig.parseArgs(args)
    }

    def printArgInfo() {
      BankingConfig.printArgs()
    }

    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx => new Teller(BankingConfig.A, BankingConfig.N, latch, ctx)),
        "Banking")
      system ! StartMessage
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }


  trait Msg extends Message
  case object StartMessage extends Msg with NoRefs
  case object StopMessage extends Msg with NoRefs
  case object ReplyMessage extends Msg with NoRefs
  case class DebitMessage(sender: ActorRef[Msg], amount: Double) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(sender)
  }
  case class CreditMessage(sender: ActorRef[Msg], amount: Double, recipient: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender, recipient)
  }

  protected class Teller(numAccounts: Int, numBankings: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val accounts = Array.tabulate[ActorRef[Msg]](numAccounts)((i) => {
      ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new Account(i, BankingConfig.INITIAL_BALANCE, ctx) })
    })
    private var numCompletedBankings = 0

    private val randomGen = new PseudoRandom(123456)

    override def process(theMsg: Msg) {
      theMsg match {

        case StartMessage =>

          var m = 0
          while (m < numBankings) {
            generateWork()
            m += 1
          }

        case ReplyMessage =>

          numCompletedBankings += 1
          if (numCompletedBankings == numBankings) {
            latch.countDown()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }

    def generateWork(): Unit = {
      // src is lower than dest id to ensure there is never a deadlock
      val srcAccountId = randomGen.nextInt((accounts.length / 10) * 8)
      var loopId = randomGen.nextInt(accounts.length - srcAccountId)
      if (loopId == 0) {
        loopId += 1
      }
      val destAccountId = srcAccountId + loopId

      val srcAccount = accounts(srcAccountId)
      val destAccount = accounts(destAccountId)
      val amount = Math.abs(randomGen.nextDouble()) * 1000

      val sender = ctx.self
      srcAccount ! CreditMessage(ctx.createRef(sender, srcAccount), amount, ctx.createRef(destAccount, srcAccount))
    }
  }

  protected class Account(id: Int, var balance: Double, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var inReplyMode = false
    private var replyTeller: ActorRef[Msg] = null
    private val stashedMessages = new ListBuffer[Msg]()

    override def process(theMsg: Msg) {

      if (inReplyMode) {

        theMsg match {

          case ReplyMessage =>

            inReplyMode = false

            replyTeller ! ReplyMessage
            if (stashedMessages.nonEmpty) {
              val newMsg = stashedMessages.remove(0)
              ctx.self ! newMsg
            }

          case message =>

            val newMsg = message match {
              case CreditMessage(sender, amount, recipient) =>
                CreditMessage(ctx.createRef(sender, ctx.self), amount, ctx.createRef(recipient, ctx.self))
              case DebitMessage(sender, amount) =>
                DebitMessage(ctx.createRef(sender, ctx.self), amount)
              case msg: NoRefs => msg
            }
            stashedMessages.append(newMsg)
        }

      } else {

        // process the message
        theMsg match {
          case dm: DebitMessage =>

            balance += dm.amount
            val creditor = dm.sender
            creditor ! ReplyMessage

          case cm: CreditMessage =>

            balance -= cm.amount
            replyTeller = cm.sender

            val sender = ctx.self
            val destAccount = cm.recipient
            destAccount ! new DebitMessage(ctx.createRef(sender, destAccount), cm.amount)
            inReplyMode = true

          case StopMessage =>
            exit()

          case message =>
            val ex = new IllegalArgumentException("Unsupported message: " + message)
            ex.printStackTrace(System.err)
        }

        // recycle stashed messages
        if (!inReplyMode && stashedMessages.nonEmpty) {
          val newMsg = stashedMessages.remove(0)
          ctx.self ! newMsg
        }
      }
    }
  }

}
