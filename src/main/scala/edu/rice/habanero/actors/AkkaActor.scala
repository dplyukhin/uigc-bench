package edu.rice.habanero.actors

import java.util.concurrent.atomic.AtomicBoolean
import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.actor.typed
import com.typesafe.config.{Config, ConfigFactory}
import edu.illinois.osl.akka.gc.{AbstractBehavior, ActorContext, Behavior, Behaviors}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.Properties._
import scala.util.{Failure, Success}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
abstract class AkkaActor[MsgType] extends Actor {

  final def receive = {
    case msg: Any =>
      process(msg.asInstanceOf[MsgType])
  }

  def process(msg: MsgType): Unit

  final def exit() = {
    onPreExit()
    context.stop(self)
    onPostExit()
  }

  /**
   * Convenience: specify code to be executed before actor is terminated
   */
  protected def onPreExit() = {
  }

  /**
   * Convenience: specify code to be executed after actor is terminated
   */
  protected def onPostExit() = {
  }
}

abstract class GCActor[MsgType](ctx: ActorContext[MsgType]) extends AbstractBehavior[MsgType](ctx) {
  private var exiting: Boolean = false

  final def onMessage(msg: MsgType): Behavior[MsgType] = {
    process(msg)
    if (exiting)
      Behaviors.stopped(ctx)
    else
      this
  }

  def process(msg: MsgType): Unit

  final def exit() = {
    onPreExit()
    exiting = true
    onPostExit()
  }

  /**
   * Convenience: specify code to be executed before actor is terminated
   */
  protected def onPreExit() = {
  }

  /**
   * Convenience: specify code to be executed after actor is terminated
   */
  protected def onPostExit() = {
  }

}

object AkkaActorState {

  private val mailboxTypeKey = "actors.mailboxType"
  private var config: Config = null

  def setPriorityMailboxType(value: String) {
    System.setProperty(mailboxTypeKey, value)
  }

  def initialize(): Unit = {

    val corePoolSize = getNumWorkers("actors.corePoolSize", 4)
    val maxPoolSize = getNumWorkers("actors.maxPoolSize", corePoolSize)
    val priorityMailboxType = getStringProp(mailboxTypeKey, "akka.dispatch.SingleConsumerOnlyUnboundedMailbox")

    val customConfigStr = """
    my-pinned-dispatcher {
      executor = "thread-pool-executor"
      type = PinnedDispatcher
    }
    akka {
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

  def newActorSystem(name: String): ActorSystem = {
    ActorSystem(name, config)
  }

  def newTypedActorSystem[T](behavior: typed.Behavior[T], name: String): typed.ActorSystem[T] = {
    typed.ActorSystem(behavior, name, config)
  }

  def awaitTermination(system: typed.ActorSystem[_]) {
    try {
      system.terminate()
    } catch {
      case ex: InterruptedException => {
        ex.printStackTrace()
      }
    }
  }

  def awaitTermination(system: ActorSystem) {
    try {
      system.terminate()
    } catch {
      case ex: InterruptedException => {
        ex.printStackTrace()
      }
    }
  }
}
