package randomgraphs

object RandomGraphsConfig {
  /** M is a configurable fixed parameter that decides the number of action an Actor will perform */
  val NumberOfActions : Int = 5
  /** N is a configurable fixed parameter that decides the amount of Actors will be generated */
  val NumberOfSpawns : Int = 50000
  /** P1 is the probability for Action 1: Spawning an Actor */
  val ProbabilityToSpawn : Double = 0.1
  /** P2 is the probability for Action 2: Sending a ref from one Actor to another*/
  val ProbabilityToSendRef : Double = 0.2
  /** P3 is the probability for Action 3: Releasing refereces to Actors */
  val ProbabilityToReleaseRef : Double = 0.4
  /** P4 is the probability for Action 4: Sending Application Messages to Actors */
  val ProbabilityToPing : Double = 0.1
  /** P5 is the probability for Action 5: Sleep */
  val ProbabilityToSleep: Double = 0
  /** Time to sleep (in nanoseconds) */
  val SleepDuration: Long = 5
  /** The number of messages from the outside world that the root actor receives per second */
  val PingsPerSecond : Int = 2000
  /** The size of each actor in bytes */
  val SizeOfActor : Int = 1000 // At 20kB/actor, the non-GC benchmark starts to crash OOM

  /** Whether actors should track information via the Statistics object */
  val LogStats : Boolean = true
  /** Whether to execute this benchmark in a single-threaded way (for debugging) */
  val IsSequential : Boolean = false
  /** Whether to log extra messages (for debugging) */
  val ShouldLog : Boolean = false
}
