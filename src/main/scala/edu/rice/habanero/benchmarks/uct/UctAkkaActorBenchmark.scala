package edu.rice.habanero.benchmarks.uct

import java.util.Random
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.uct.UctConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object UctAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new UctAkkaActorBenchmark)
  }

  private final class UctAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      UctConfig.parseArgs(args)
    }

    def printArgInfo() {
      UctConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(
        Behaviors.setupRoot(ctx =>
          new RootActor(latch, ctx)
        ),
        "UCT")
      system ! GenerateTreeMessage
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private trait Msg extends Message
  private case class ParentRootMessage(parent: ActorRef[Msg], root: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(parent, root)
  }
  private case object GetIdMessage extends Msg with NoRefs
  private case object PrintInfoMessage extends Msg with NoRefs
  private case object GenerateTreeMessage extends Msg with NoRefs
  private case object TryGenerateChildrenMessage extends Msg with NoRefs
  private case class GenerateChildrenMessage(currentId: Int, compSize: Int) extends Msg with NoRefs
  private case class UrgentGenerateChildrenMessage(urgentChildId: Int, currentId: Int, compSize: Int) extends Msg with NoRefs
  private case object TraverseMessage extends Msg with NoRefs
  private case object UrgentTraverseMessage extends Msg with NoRefs
  private case class ShouldGenerateChildrenMessage(sender: ActorRef[Msg], childHeight: Int) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(sender)
  }
  private case class UpdateGrantMessage(childId: Int) extends Msg with NoRefs
  private case object TerminateMessage extends Msg with NoRefs

  /**
   * @author xinghuizhao
   * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
   */
  private class RootActor(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private final val ran: Random = new Random(2)
    private var height: Int = 1
    private var size: Int = 1
    private final val children = new Array[ActorRef[Msg]](UctConfig.BINOMIAL_PARAM)
    private final val hasGrantChildren = new Array[Boolean](UctConfig.BINOMIAL_PARAM)
    private var traversed: Boolean = false
    private var finalSizePrinted: Boolean = false

    override def process(theMsg: Msg) {
      theMsg match {
        case GenerateTreeMessage =>
          generateTree()
        case grantMessage: UpdateGrantMessage =>
          updateGrant(grantMessage.childId)
        case booleanMessage: ShouldGenerateChildrenMessage =>
          val sender: ActorRef[Msg] = booleanMessage.sender
          checkGenerateChildrenRequest(sender, booleanMessage.childHeight)
        case PrintInfoMessage =>
          printInfo()
        case TerminateMessage =>
          terminateMe()
        case _ =>
      }
    }

    /**
     * This message is called externally to create the BINOMIAL_PARAM tree
     */
    def generateTree() {
      height += 1
      val computationSize: Int = getNextNormal(UctConfig.AVG_COMP_SIZE, UctConfig.STDEV_COMP_SIZE)

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        hasGrantChildren(i) = false
        children(i) = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx =>
          new NodeActor(height, size + i, computationSize, urgent = false, ctx)
        })
        children(i) ! ParentRootMessage(ctx.createRef(ctx.self, children(i)), ctx.createRef(ctx.self, children(i)))

        i += 1
      }
      size += UctConfig.BINOMIAL_PARAM

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {

        children(j) ! TryGenerateChildrenMessage

        j += 1
      }
    }

    /**
     * This message is called by a child node before generating children;
     * the child may generate children only if this message returns true
     *
     * @param childName The child name
     * @param childHeight The height of the child in the tree
     */
    def checkGenerateChildrenRequest(childName: ActorRef[Msg], childHeight: Int) {
      if (size + UctConfig.BINOMIAL_PARAM <= UctConfig.MAX_NODES) {
        val moreChildren: Boolean = ran.nextBoolean
        if (moreChildren) {
          val childComp: Int = getNextNormal(UctConfig.AVG_COMP_SIZE, UctConfig.STDEV_COMP_SIZE)
          val randomInt: Int = ran.nextInt(100)
          if (randomInt > UctConfig.URGENT_NODE_PERCENT) {
            childName ! new GenerateChildrenMessage(size, childComp)
          } else {
            childName ! new UrgentGenerateChildrenMessage(ran.nextInt(UctConfig.BINOMIAL_PARAM), size, childComp)
          }
          size += UctConfig.BINOMIAL_PARAM
          if (childHeight + 1 > height) {
            height = childHeight + 1
          }
        }
        else {
          if (childHeight > height) {
            height = childHeight
          }
        }
      }
      else {
        if (!finalSizePrinted) {
          System.out.println("final size= " + size)
          System.out.println("final height= " + height)
          finalSizePrinted = true
        }
        if (!traversed) {
          traversed = true
          traverse()
        }
        latch.countDown()
      }
    }

    /**
     * This method is called by getBoolean in order to generate computation times for actors, which
     * follows a normal distribution with mean value and a std value
     */
    def getNextNormal(pMean: Int, pDev: Int): Int = {
      var result: Int = 0
      while (result <= 0) {
        val tempDouble: Double = ran.nextGaussian * pDev + pMean
        result = Math.round(tempDouble).asInstanceOf[Int]
      }
      result
    }

    /**
     * This message is called by a child node to indicate that it has children
     */
    def updateGrant(childId: Int) {
      hasGrantChildren(childId) = true
    }

    /**
     * This is the method for traversing the tree
     */
    def traverse() {
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        children(i) ! TraverseMessage
        i += 1
      }
    }

    def printInfo() {
      System.out.println("0 0 children starts 1")
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        children(i) ! PrintInfoMessage
        i += 1
      }

    }

    def terminateMe() {
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        children(i) ! TerminateMessage
        i += 1
      }

      exit()
    }
  }

  /**
   * @author xinghuizhao
   * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
   */
  protected object NodeActor {

    private final val dummy: Int = 40000
  }

  private class NodeActor(myHeight: Int, myId: Int, myCompSize: Int, urgent: Boolean, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var myRoot: ActorRef[Msg] = null
    private var myParent: ActorRef[Msg] = null
    private var urgentChild: Int = 0
    private var hasChildren: Boolean = false
    private final val children = new Array[ActorRef[Msg]](UctConfig.BINOMIAL_PARAM)
    private final val hasGrantChildren = new Array[Boolean](UctConfig.BINOMIAL_PARAM)

    override def process(theMsg: Msg) {
      theMsg match {
        case ParentRootMessage(parent, root) =>
          myParent = parent
          myRoot = root
        case TryGenerateChildrenMessage =>
          tryGenerateChildren()
        case childrenMessage: GenerateChildrenMessage =>
          generateChildren(childrenMessage.currentId, childrenMessage.compSize)
        case childrenMessage: UrgentGenerateChildrenMessage =>
          generateUrgentChildren(childrenMessage.urgentChildId, childrenMessage.currentId, childrenMessage.compSize)
        case grantMessage: UpdateGrantMessage =>
          updateGrant(grantMessage.childId)
        case TraverseMessage =>
          traverse()
        case UrgentTraverseMessage =>
          urgentTraverse()
        case PrintInfoMessage =>
          printInfo()
        case GetIdMessage =>
          getId
        case TerminateMessage =>
          terminateMe()
        case _ =>
      }
    }

    /**
     * This message is called by parent node, try to generate children of this node.
     * If the "getBoolean" message returns true, the node is allowed to generate BINOMIAL_PARAM children
     */
    def tryGenerateChildren() {
      UctConfig.loop(100, NodeActor.dummy)
      myRoot ! new ShouldGenerateChildrenMessage(ctx.createRef(ctx.self, myRoot), myHeight)
    }

    def generateChildren(currentId: Int, compSize: Int) {
      val myArrayId: Int = myId % UctConfig.BINOMIAL_PARAM
      myParent ! new UpdateGrantMessage(myArrayId)
      val childrenHeight: Int = myHeight + 1
      val idValue: Int = currentId

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        children(i) = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx =>
          new NodeActor(childrenHeight, idValue + i, compSize, urgent = false, ctx)
        })
        children(i) ! ParentRootMessage(ctx.createRef(ctx.self, children(i)), ctx.createRef(myRoot, children(i)))
        i += 1
      }

      hasChildren = true

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {

        children(j) ! TryGenerateChildrenMessage
        j += 1
      }
    }

    def generateUrgentChildren(urgentChildId: Int, currentId: Int, compSize: Int) {
      val myArrayId: Int = myId % UctConfig.BINOMIAL_PARAM
      myParent ! new UpdateGrantMessage(myArrayId)
      val childrenHeight: Int = myHeight + 1
      val idValue: Int = currentId
      urgentChild = urgentChildId

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        children(i) = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx =>
          new NodeActor(childrenHeight, idValue + i, compSize, i == urgentChild, ctx)
        })
        children(i) ! ParentRootMessage(ctx.createRef(ctx.self, children(i)), ctx.createRef(myRoot, children(i)))
        i += 1
      }

      hasChildren = true

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {

        children(j) ! TryGenerateChildrenMessage
        j += 1
      }
    }

    /**
     * This message is called by a child node to indicate that it has children
     */
    def updateGrant(childId: Int) {
      hasGrantChildren(childId) = true
    }

    /**
     * This message is called by parent while doing a traverse
     */
    def traverse() {
      UctConfig.loop(myCompSize, NodeActor.dummy)
      if (hasChildren) {

        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {

          children(i) ! TraverseMessage
          i += 1
        }
      }
    }

    /**
     * This message is called by parent while doing traverse, if this node is an urgent node
     */
    def urgentTraverse() {
      UctConfig.loop(myCompSize, NodeActor.dummy)
      if (hasChildren) {
        if (urgentChild != -1) {

          var i: Int = 0
          while (i < UctConfig.BINOMIAL_PARAM) {
            if (i != urgentChild) {
              children(i) ! TraverseMessage
            } else {
              children(urgentChild) ! UrgentTraverseMessage
            }
            i += 1
          }
        } else {

          var i: Int = 0
          while (i < UctConfig.BINOMIAL_PARAM) {
            children(i) ! TraverseMessage
            i += 1
          }
        }
      }
      if (urgent) {
        System.out.println("urgent traverse node " + myId + " " + System.currentTimeMillis)
      } else {
        System.out.println(myId + " " + System.currentTimeMillis)
      }
    }

    def printInfo() {
      if (urgent) {
        System.out.print("Urgent......")
      }
      if (hasChildren) {
        System.out.println(myId + " " + myCompSize + "  children starts ")

        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {
          children(i) ! PrintInfoMessage
          i += 1
        }
      } else {
        System.out.println(myId + " " + myCompSize)
      }
    }

    def getId: Int = {
      myId
    }

    def terminateMe() {
      if (hasChildren) {

        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {
          children(i) ! TerminateMessage
          i += 1
        }
      }
      exit()
    }
  }

}
