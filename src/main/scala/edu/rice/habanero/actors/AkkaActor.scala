package edu.rice.habanero.actors

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.typed
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import gc.{AnyActorRef, AppMsg, GCMessage, Message}
import akka.actor.typed.{ActorSystem, Behavior}
import com.typesafe.config.{Config, ConfigFactory}
import edu.rice.hj.runtime.util.ModCountDownLatch

import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.Properties._
import scala.util.{Failure, Success}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
abstract class AkkaActor[T](context: ActorContext[AkkaMsg[T]]) extends AbstractBehavior[AkkaMsg[T]](context) {

  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)

  final override def onMessage(msg: AkkaMsg[T]): Behavior[AkkaMsg[T]] =
    msg match{
      case msg: StartAkkaActorMessage =>
        if (hasStarted) {
          msg.resolve(value = false)
        } else {
          start()
          msg.resolve(value = true)
        }
        this
      case BenchmarkMessage(payload) =>
        if (!exitTracker.get()) {
          process(payload)
        }
        this
    }

  def process(msg: T): Behavior[AkkaMsg[T]]

  def send(msg: Nothing) {
    context.self ! msg
  }

  final def hasStarted: Boolean = {
    startTracker.get()
  }

  final def start(): Unit = {
    if (!hasStarted) {
      onPreStart()
      onPostStart()
      startTracker.set(true)
    }
  }

  /**
   * Convenience: specify code to be executed before actor is started
   */
  protected def onPreStart(): Unit = {
  }

  /**
   * Convenience: specify code to be executed after actor is started
   */
  protected def onPostStart(): Unit = {
  }

  final def hasExited: Boolean = {
    exitTracker.get()
  }

  final def exit(): Behavior[AkkaMsg[T]] = {
    val success = exitTracker.compareAndSet(false, true)
    if (success) {
      AkkaActorState.actorLatch.countDown()
      Behaviors.stopped
    }
    this
  }
}

sealed trait AkkaMsg[+T] extends Message

final case class StartAkkaActorMessage(promise: Promise[Boolean]) extends AkkaMsg[Nothing] {
  def refs = Seq()

  def await() {
    Await.result(promise.future, Duration.Inf)
  }

  def resolve(value: Boolean) {
    promise.success(value)
  }
}

final case class BenchmarkMessage[T <: Message](payload: T) extends AkkaMsg[T] {
  def refs: Iterable[AnyActorRef] = payload.refs
}

object AkkaImplicits {
  implicit def liftBenchmarkMessage[T <: Message](msg : T) : BenchmarkMessage[T] =
    BenchmarkMessage(msg)
}

object AkkaActorState {

  val actorLatch = new ModCountDownLatch(0)

  private val mailboxTypeKey = "actors.mailboxType"
  var config: Config = null

  def setPriorityMailboxType(value: String) {
    System.setProperty(mailboxTypeKey, value)
  }

  def initialize(): Unit = {

    val corePoolSize = getNumWorkers("actors.corePoolSize", 4)
    val maxPoolSize = getNumWorkers("actors.maxPoolSize", corePoolSize)
    val priorityMailboxType = getStringProp(mailboxTypeKey, "akka.dispatch.SingleConsumerOnlyUnboundedMailbox")

    val customConfigStr = """
    akka {
      stdout-loglevel = "WARNING"
      loglevel = "WARNING"
      log-dead-letters-during-shutdown = off
      log-dead-letters = off

      actor {
        creation-timeout = 6000s
        default-dispatcher {
          fork-join-executor {
            parallelism-min = %s
            parallelism-max = %s
            parallelism-factor = 1.0
          }
        }
        default-mailbox {
          mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
        }
        prio-dispatcher {
          mailbox-type = "%s"
        }
        typed {
          timeout = 10000s
        }
      }
    }
                          """.format(corePoolSize, maxPoolSize, priorityMailboxType)

    // println(customConfigStr)

    val customConf = ConfigFactory.parseString(customConfigStr)
    config = ConfigFactory.load(customConf)

  }

  private def getNumWorkers(propertyName: String, minNumThreads: Int): Int = {
    val rt: Runtime = java.lang.Runtime.getRuntime

    getIntegerProp(propertyName) match {
      case Some(i) if i > 0 => i
      case _ => {
        val byCores = rt.availableProcessors() * 2
        if (byCores > minNumThreads) byCores else minNumThreads
      }
    }
  }


  private def getIntegerProp(propName: String): Option[Int] = {
    try {
      propOrNone(propName) map (_.toInt)
    } catch {
      case _: SecurityException | _: NumberFormatException => None
    }
  }

  private def getStringProp(propName: String, defaultVal: String): String = {
    propOrElse(propName, defaultVal)
  }

  def newActorSystem[T](name: String, behavior : typed.Behavior[T]): ActorSystem[T] = {
    ActorSystem(behavior, name, config)
  }

  def startActor[T](actorRef: typed.ActorRef[AkkaMsg[T]]): Unit = {

    AkkaActorState.actorLatch.updateCount()

    val promise = Promise[Boolean]()
    val message = new StartAkkaActorMessage(promise)
    actorRef ! message

    val f = promise.future
    f.onComplete {
      case Success(value) =>
        if (!value) {
          AkkaActorState.actorLatch.countDown()
        }
      case Failure(e) => e.printStackTrace()
    }

  }

  def startActor[T](actorRef: gc.ActorRef[AkkaMsg[T]]): Unit = {

    AkkaActorState.actorLatch.updateCount()

    val promise = Promise[Boolean]()
    val message = new StartAkkaActorMessage(promise)
    actorRef ! message

    val f = promise.future
    f.onComplete {
      case Success(value) =>
        if (!value) {
          AkkaActorState.actorLatch.countDown()
        }
      case Failure(e) => e.printStackTrace()
    }

  }

  def awaitTermination(system: ActorSystem[Nothing]) {
    try {
      actorLatch.await()
      system.terminate()
    } catch {
      case ex: InterruptedException => {
        ex.printStackTrace()
      }
    }
  }
}
