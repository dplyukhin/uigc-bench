package edu.rice.habanero.benchmarks.facloc

import java.util
import java.util.function.Consumer
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.uigc.actor.typed._
import org.apache.pekko.uigc.actor.typed.scaladsl._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, GCActor}
import edu.rice.habanero.benchmarks.facloc.FacilityLocationConfig.{Box, Point, Position}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import java.util.concurrent.CountDownLatch

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FacilityLocationAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FacilityLocationAkkaActorBenchmark)
  }

  private final class FacilityLocationAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FacilityLocationConfig.parseArgs(args)
    }

    def printArgInfo() {
      FacilityLocationConfig.printArgs()
    }
    private var system: ActorSystem[Msg] = _
    def runIteration() {

      val latch = new CountDownLatch(1)
      system = AkkaActorState.newTypedActorSystem(Behaviors.setupRoot[Msg](ctx => new Master(latch, ctx)), "FacilityLocation")
      latch.await()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }


  private abstract class Msg() extends Message
  private case class Rfmsg(actor: ActorRef[Msg]) extends Msg {
    override def refs: Iterable[ActorRef[_]] = Some(actor)
  }
  private case class FacilityMsg(positionRelativeToParent: Int, depth: Int, point: Point, fromChild: Boolean) extends
    Msg with NoRefs
  private case class NextCustomerMsg() extends Msg with NoRefs
  private case class CustomerMsg(producer: ActorRef[Msg], point: Point) extends Msg {
    override def refs: Iterable[ActorRef[_]] = List(producer)
  }
  private case class RequestExitMsg() extends Msg with NoRefs
  private case class ConfirmExitMsg(facilities: Int, supportCustomers: Int) extends Msg with NoRefs


  private class Master(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {
    {

      val threshold = FacilityLocationConfig.ALPHA * FacilityLocationConfig.F
      val boundingBox = new Box(0, 0, FacilityLocationConfig.GRID_SIZE, FacilityLocationConfig.GRID_SIZE)

      val rootQuadrant = ctx.spawnAnonymous(Behaviors.setup[Msg]( ctx => new QuadrantActor(
        Position.ROOT, boundingBox, threshold, 0,
        new java.util.ArrayList[Point](), 1, -1, new java.util.ArrayList[Point](), ctx)))

      val producer = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new ProducerActor(latch, ctx) })
      producer ! Rfmsg(ctx.createRef(rootQuadrant, producer))
    }
    override def process(msg: Msg): Unit = ()
  }

  private class ProducerActor(latch: CountDownLatch, ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var consumer: ActorRef[Msg] = _
    private var itemsProduced = 0

    private def produceCustomer(): Unit = {
      consumer ! CustomerMsg(ctx.createRef(ctx.self, consumer), Point.random(FacilityLocationConfig.GRID_SIZE))
      itemsProduced += 1
    }

    override def process(message: Msg) {
      message match {
        case Rfmsg(x) =>
          this.consumer = x
          produceCustomer()
        case msg: NextCustomerMsg =>
          if (itemsProduced < FacilityLocationConfig.NUM_POINTS) {
            produceCustomer()
          } else {
            latch.countDown()
          }
      }
    }
  }

  private class QuadrantActor(positionRelativeToParent: Int,
                              val boundary: Box,
                              threshold: Double,
                              depth: Int,
                              initLocalFacilities: java.util.List[Point],
                              initKnownFacilities: Int,
                              initMaxDepthOfKnownOpenFacility: Int,
                              initCustomers: java.util.List[Point], ctx: ActorContext[Msg]) extends GCActor[Msg](ctx) {

    private var parent: ActorRef[Msg] = _
    // the facility associated with this quadrant if it were to open
    private val facility: Point = boundary.midPoint()

    // all the local facilities from corner ancestors
    val localFacilities = new java.util.ArrayList[Point]()
    localFacilities.addAll(initLocalFacilities)
    localFacilities.add(facility)

    private var knownFacilities = initKnownFacilities
    private var maxDepthOfKnownOpenFacility = initMaxDepthOfKnownOpenFacility
    private var terminatedChildCount = 0

    // the support customers for this Quadrant
    private val supportCustomers = new java.util.ArrayList[Point]()

    private var childrenFacilities = 0
    private var facilityCustomers = 0

    // null when closed, non-null when open
    private var children: List[ActorRef[Msg]] = null
    private var childrenBoundaries: List[Box] = null

    // the cost so far
    private var totalCost = 0.0

    initCustomers.forEach(new Consumer[Point] {
      override def accept(loopPoint: Point): Unit = {
        if (boundary.contains(loopPoint)) {
          addCustomer(loopPoint)
        }
      }

      override def andThen(after: Consumer[_ >: Point]): Consumer[Point] = {
        this
      }
    })

    override def process(msg: Msg) {
      msg match {
        case Rfmsg(x) =>
          this.parent = x

        case customer: CustomerMsg =>

          val producer = customer.producer
          val point: Point = customer.point
          if (children == null) {

            // no open facility
            addCustomer(point)
            if (totalCost > threshold) {
              partition()
            }

          } else {

            // a facility is already open, propagate customer to correct child
            var index = 0
            while (index <= 4) {
              val loopChildBoundary = childrenBoundaries(index)
              if (loopChildBoundary.contains(point)) {
                children(index) ! CustomerMsg(ctx.createRef(producer, children(index)), point)
                index = 5
              } else {
                index += 1
              }
            }
          }

          if (parent eq null) {
            // request next customer
            customer.producer ! (NextCustomerMsg())
          }

        case facility: FacilityMsg =>

          val point = facility.point
          val fromChild = facility.fromChild

          knownFacilities += 1
          localFacilities.add(point)

          if (fromChild) {
            notifyParentOfFacility(point, facility.depth)
            if (facility.depth > maxDepthOfKnownOpenFacility) {
              maxDepthOfKnownOpenFacility = facility.depth
            }

            // notify sibling
            val childPos = facility.positionRelativeToParent
            val siblingPos: Int = if (childPos == Position.TOP_LEFT) {
              Position.BOT_RIGHT
            } else if (childPos == Position.TOP_RIGHT) {
              Position.BOT_LEFT
            } else if (childPos == Position.BOT_RIGHT) {
              Position.TOP_LEFT
            } else {
              Position.TOP_RIGHT
            }
            children(siblingPos) ! (FacilityMsg(Position.UNKNOWN, depth, point, false))

          } else {

            // notify all children
            if (children ne null) {
              children.foreach {
                loopChild =>
                  loopChild ! (FacilityMsg(Position.UNKNOWN, depth, point, false))
              }
            }
          }

        case exitMsg: RequestExitMsg =>

          if (children ne null) {
            children.foreach {
              loopChild =>
                loopChild ! (exitMsg)
            }
          } else {
            // No children, notify parent and safely exit
            safelyExit()
          }

        case exitMsg: ConfirmExitMsg =>

          // child has sent a confirmation that it has exited
          terminatedChildCount += 1

          childrenFacilities += exitMsg.facilities
          facilityCustomers += exitMsg.supportCustomers

          if (terminatedChildCount == 4) {
            // all children terminated
            safelyExit()
          }
      }
    }

    private def addCustomer(point: Point): Unit = {
      supportCustomers.add(point)
      val minCost = findCost(point)
      totalCost += minCost
    }

    private def findCost(point: Point): Double = {
      var result = Double.MaxValue

      // there will be at least one facility
      localFacilities.forEach(new Consumer[Point] {
        override def accept(loopPoint: Point): Unit = {
          val distance = loopPoint.getDistance(point)
          if (distance < result) {
            result = distance
          }
        }

        override def andThen(after: Consumer[_ >: Point]): Consumer[Point] = {
          this
        }
      })

      result
    }

    private def notifyParentOfFacility(p: Point, depth: Int): Unit = {
      //println("Quadrant-" + id + ": notifyParentOfFacility: parent = " + parent)
      if (parent ne null) {
        //println("Quadrant-" + id + ": notifyParentOfFacility: sending msg to parent: " + parent.id)
        parent ! (FacilityMsg(positionRelativeToParent, depth, p, true))
      }
    }

    private def partition(): Unit = {

      // notify parent that opened a new facility
      notifyParentOfFacility(facility, depth)
      maxDepthOfKnownOpenFacility = math.max(maxDepthOfKnownOpenFacility, depth)

      // create children and propagate their share of customers to them
      val firstBoundary: Box = new Box(boundary.x1, facility.y, facility.x, boundary.y2)
      val secondBoundary: Box = new Box(facility.x, facility.y, boundary.x2, boundary.y2)
      val thirdBoundary: Box = new Box(boundary.x1, boundary.y1, facility.x, facility.y)
      val fourthBoundary: Box = new Box(facility.x, boundary.y1, boundary.x2, facility.y)

      val customers1 = new util.ArrayList[Point](supportCustomers)
      val firstChild = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new QuadrantActor(
        Position.TOP_LEFT, firstBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, customers1, ctx)})
      firstChild ! Rfmsg(ctx.createRef(ctx.self, firstChild))

      val customers2 = new util.ArrayList[Point](supportCustomers)
      val secondChild = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new QuadrantActor(
        Position.TOP_RIGHT, secondBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, customers2, ctx)})
      secondChild ! Rfmsg(ctx.createRef(ctx.self, secondChild))

      val customers3 = new util.ArrayList[Point](supportCustomers)
      val thirdChild = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new QuadrantActor(
        Position.BOT_LEFT, thirdBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, customers3, ctx)})
      thirdChild ! Rfmsg(ctx.createRef(ctx.self, thirdChild))

      val customers4 = new util.ArrayList[Point](supportCustomers)
      val fourthChild = ctx.spawnAnonymous(Behaviors.setup[Msg] { ctx => new QuadrantActor(
        Position.BOT_RIGHT, fourthBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, customers4, ctx)})
      fourthChild ! Rfmsg(ctx.createRef(ctx.self, fourthChild))

      children = List[ActorRef[Msg]](firstChild, secondChild, thirdChild, fourthChild)
      childrenBoundaries = List[Box](firstBoundary, secondBoundary, thirdBoundary, fourthBoundary)

      // support customers have been distributed to the children
      supportCustomers.clear()
    }

    private def safelyExit(): Unit = {

      if (parent ne null) {
        val numFacilities = if (children ne null) childrenFacilities + 1 else childrenFacilities
        val numCustomers = facilityCustomers + supportCustomers.size
        parent ! (ConfirmExitMsg(numFacilities, numCustomers))
      } else {
        val numFacilities = childrenFacilities + 1
        println("  Num Facilities: " + numFacilities + ", Num customers: " + facilityCustomers)
      }
      exit()

    }
  }

}
