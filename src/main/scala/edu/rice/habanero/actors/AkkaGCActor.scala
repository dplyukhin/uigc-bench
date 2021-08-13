package edu.rice.habanero.actors

import java.util.concurrent.atomic.AtomicBoolean
import akka.actor.typed.scaladsl.Behaviors
import gc.{AbstractBehavior, ActorContext, ActorRef, AnyActorRef, Behavior, Message}


abstract class AkkaGCActor[T](context: ActorContext[AkkaMsg[T]]) extends AbstractBehavior[AkkaMsg[T]](context) {

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
