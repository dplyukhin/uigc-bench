package meta

import java.util.concurrent.CountDownLatch

class IterationInfo(
  val role: String,
  val readyLatch: CountDownLatch,
  val doneLatch: CountDownLatch,
  val iteration: Int,
  val numNodes: Int,
  val isWarmup: Boolean
)
