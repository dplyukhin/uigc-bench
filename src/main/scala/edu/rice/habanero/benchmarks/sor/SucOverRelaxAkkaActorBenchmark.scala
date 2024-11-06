package edu.rice.habanero.benchmarks.sor

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SucOverRelaxAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SucOverRelaxAkkaActorBenchmark)
  }

  private final class SucOverRelaxAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SucOverRelaxConfig.parseArgs(args)
    }

    def printArgInfo() {
      SucOverRelaxConfig.printArgs()
      SucOverRelaxConfig.initialize()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {
      val dataLevel = SucOverRelaxConfig.N
      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new SorRunner(dataLevel, latch, ctx)
        ),
        "SucOverRelax")
      system ! SorBootMessage
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      AkkaActorState.awaitTermination(system)
      SucOverRelaxConfig.initialize()
    }
  }

  trait Msg extends Message
  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  private case class PeerMsg(actor: ActorRef[Msg], border: Array[ActorRef[Msg]]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Iterable(actor) ++ border.view
  }
  case class SorBorderMessage(mBorder: Array[ActorRef[Msg]]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = mBorder
  }
  case class SorStartMessage(mi: Int, mActors: Array[ActorRef[Msg]]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = mActors
  }
  case class SorValueMessage(v: Double) extends Msg with NoRefs
  case object SorBootMessage extends Msg with NoRefs
  case class SorResultMessage(mx: Int, my: Int, mv: Double, msgRcv: Int) extends Msg with NoRefs

  private class SorRunner(n: Int, latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private val s = SucOverRelaxConfig.DATA_SIZES(n)
    private val part = s / 2
    private val sorActors = Array.ofDim[ActorRef[Msg]](s * (part + 1))

    private def boot(): Unit = {

      val myBorder = Array.ofDim[ActorRef[Msg]](s)
      val randoms = SucOverRelaxConfig.A

      for (i <- 0 until s) {
        var c = i % 2
        for (j <- 0 until part) {
          val pos = i * (part + 1) + j
          c = 1 - c
          sorActors(pos) = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SorActor(pos, randoms(i)(j), c, s, part + 1, SucOverRelaxConfig.OMEGA, false, ctx)})
          sorActors(pos) ! Rfmsg(ctx.createRef(ctx.self, sorActors(pos)))
          if (j == (part - 1)) {
            myBorder(i) = sorActors(pos)
          }
        }
      }

      val partialMatrix = Array.ofDim[Double](s, s - part)
      for (i <- 0 until s) {
        for (j <- 0 until s - part) {
          partialMatrix(i)(j) = randoms(i)(j + part)
        }
      }

      val sorPeer = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new SorPeer(s, part, partialMatrix, ctx)})
      val borderRefs = myBorder.map(ctx.createRef(_, sorPeer))
      sorPeer ! PeerMsg(ctx.createRef(ctx.self, sorPeer), borderRefs)
      sorPeer ! SorBootMessage
    }

    private var gTotal = 0.0
    private var returned = 0
    private var totalMsgRcv = 0
    private var expectingBoot = true

    override def process(msg: Msg) {
      msg match {
        case SorBootMessage =>
          expectingBoot = false
          boot()
        case SorResultMessage(mx, my, mv, msgRcv) =>
          if (expectingBoot) {
            throw new IllegalStateException("SorRunner not booted yet!")
          }
          totalMsgRcv += msgRcv
          returned += 1
          gTotal += mv
          if (returned == (s * part) + 1) {
            SucOverRelaxConfig.jgfValidate(gTotal, n)
            latch.countDown()
          }
        case SorBorderMessage(mBorder) =>
          if (expectingBoot) {
            throw new IllegalStateException("SorRunner not booted yet!")
          }
          for (i <- 0 until s) {
            sorActors((i + 1) * (part + 1) - 1) = mBorder(i)
          }
          for (i <- 0 until s) {
            for (j <- 0 until part) {
              val pos = i * (part + 1) + j
              val sorRefs = sorActors.map(ctx.createRef(_, sorActors(pos)))
              sorActors(pos) ! SorStartMessage(SucOverRelaxConfig.JACOBI_NUM_ITER, sorRefs)
            }
          }
      }
    }
  }

  class SorActor(
                  pos: Int,
                  var value: Double,
                  color: Int,
                  nx: Int,
                  ny: Int,
                  omega: Double,
                  peer: Boolean,
                  ctx: ActorContext[Msg]
                  ) extends GCActor[Msg](ctx) {
    private var sorSource: ActorRef[Msg] = _

    private final val x = pos / ny
    private final val y = pos % ny

    private final val omega_over_four = 0.25 * omega
    private final val one_minus_omega = 1.0 - omega

    private final val neighbors: Array[Int] =
      if (x > 0 && x < nx - 1 && y > 0 && y < ny - 1) {
        val tempNeighbors = Array.ofDim[Int](4)
        tempNeighbors(0) = calPos(x, y + 1)
        tempNeighbors(1) = calPos(x + 1, y)
        tempNeighbors(2) = calPos(x, y - 1)
        tempNeighbors(3) = calPos(x - 1, y)
        tempNeighbors
      } else if ((x == 0 || x == (nx - 1)) && (y == 0 || y == (ny - 1))) {
        val tempNeighbors = Array.ofDim[Int](2)
        tempNeighbors(0) = if (x == 0) calPos(x + 1, y) else calPos(x - 1, y)
        tempNeighbors(1) = if (y == 0) calPos(x, y + 1) else calPos(x, y - 1)
        tempNeighbors
      } else if ((x == 0 || x == (nx - 1)) || (y == 0 || y == (ny - 1))) {
        val tempNeighbors = Array.ofDim[Int](3)
        if (x == 0 || x == nx - 1) {
          tempNeighbors(0) = if (x == 0) calPos(x + 1, y) else calPos(x - 1, y)
          tempNeighbors(1) = calPos(x, y + 1)
          tempNeighbors(2) = calPos(x, y - 1)
        } else {
          tempNeighbors(0) = if (y == 0) calPos(x, y + 1) else calPos(x, y - 1)
          tempNeighbors(1) = calPos(x + 1, y)
          tempNeighbors(2) = calPos(x - 1, y)
        }
        tempNeighbors
      } else {
        Array.ofDim[Int](0)
      }

    private def calPos(x1: Int, y1: Int): Int = {
      x1 * ny + y1
    }

    private var iter = 0
    private var maxIter = 0
    private var msgRcv = 0
    private var sorActors: Array[ActorRef[Msg]] = null

    private var receivedVals = 0
    private var sum = 0.0
    private var expectingStart = true
    private val pendingMessages = new ListBuffer[Msg with NoRefs]()

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(actor) =>
          sorSource = actor
        case SorStartMessage(mi, mActors) =>
          expectingStart = false
          sorActors = mActors
          maxIter = mi
          if (color == 1) {
            neighbors.foreach {
              loopNeighIndex =>
                sorActors(loopNeighIndex) ! SorValueMessage(value)
            }
            iter += 1
            msgRcv += 1
          }
          pendingMessages.foreach {
            loopMessage => ctx.self ! loopMessage
          }
          pendingMessages.clear()

        case message: SorValueMessage =>
          if (expectingStart) {
            pendingMessages.append(message)
          } else {
            msgRcv += 1
            if (iter < maxIter) {
              receivedVals += 1
              sum += message.v
              if (receivedVals == neighbors.length) {
                value = (omega_over_four * sum) + (one_minus_omega * value)
                sum = 0.0
                receivedVals = 0

                neighbors.foreach {
                  loopNeighIndex =>
                    sorActors(loopNeighIndex) ! SorValueMessage(value)
                }
                iter += 1
              }
              if (iter >= maxIter) {
                sorSource ! SorResultMessage(x, y, value, msgRcv)
                exit()
              }
            }
          }
      }
    }
  }

  class SorPeer(
                 s: Int,
                 partStart: Int,
                 matrixPart: Array[Array[Double]],
                 ctx: ActorContext[Msg]
                 ) extends GCActor[Msg](ctx) {

    private var sorSource: ActorRef[Msg] = _
    private var border: Array[ActorRef[Msg]] = _
    private val sorActors = Array.ofDim[ActorRef[Msg]](s * (s - partStart + 1))

    private def boot(): Unit = {
      val myBorder = Array.ofDim[ActorRef[Msg]](s)
      for (i <- 0 until s) {
        sorActors(i * (s - partStart + 1)) = border(i)
      }
      for (i <- 0 until s) {
        var c = (i + partStart) % 2
        for (j <- 1 until (s - partStart + 1)) {
          val pos = i * (s - partStart + 1) + j
          c = 1 - c
          sorActors(pos) = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx =>
            new SorActor(pos, matrixPart(i)(j - 1), c, s, s - partStart + 1, SucOverRelaxConfig.OMEGA, true, ctx)})
          sorActors(pos) ! Rfmsg(ctx.createRef(ctx.self, sorActors(pos)))

          if (j == 1) {
            myBorder(i) = sorActors(pos)
          }
        }
      }
      val borderRefs = myBorder.map(ctx.createRef(_, sorSource))
      sorSource ! SorBorderMessage(borderRefs)

      for (i <- 0 until s) {
        for (j <- 1 until (s - partStart + 1)) {
          val pos = i * (s - partStart + 1) + j
          val sorRefs = sorActors.map(ctx.createRef(_, sorActors(pos)))
          sorActors(pos) ! SorStartMessage(SucOverRelaxConfig.JACOBI_NUM_ITER, sorRefs)
        }
      }
    }

    private var gTotal = 0.0
    private var returned = 0
    private var totalMsgRcv = 0
    private var expectingBoot = true

    override def process(msg: Msg) {
      msg match {
        case PeerMsg(actor, border) =>
          sorSource = actor
          this.border = border
        case SorBootMessage =>
          expectingBoot = false
          boot()
        case SorResultMessage(mx, my, mv, msgRcv) =>
          if (expectingBoot) {
            throw new IllegalStateException("SorPeer not booted yet!")
          }
          totalMsgRcv += msgRcv
          returned += 1
          gTotal += mv
          if (returned == s * (s - partStart)) {
            sorSource ! SorResultMessage(-1, -1, gTotal, totalMsgRcv)
            exit()
          }
      }
    }
  }

}
