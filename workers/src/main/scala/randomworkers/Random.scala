package randomworkers

import scala.collection.mutable

private class Random() {

  private val rng = new java.util.Random(System.currentTimeMillis())

  def roll(probability: Double): Boolean =
    rng.nextDouble() < probability

  def genData(size: Int): List[Int] =
    List.tabulate(rng.nextInt(size))(i => i)

  def selectDistinct[T](
      items: mutable.ArrayBuffer[T],
      bound: Int,
      chosen: Set[Any] = Set()
  ): Iterable[T] = {
    if (items.isEmpty) return Nil
    if (bound == 0) return chosen.asInstanceOf[Iterable[T]]
    val item = select(items)
    if (chosen contains item)
      selectDistinct(items, bound - 1, chosen)
    else
      selectDistinct(items, bound - 1, chosen + item)
  }

  def select[T](items: mutable.ArrayBuffer[T], bound: Int): Iterable[T] = {
    if (items.isEmpty) return Nil
    val numItems = rng.nextInt(bound + 1)
    (1 to numItems).map(_ => select(items))
  }

  def select[T](items: mutable.ArrayBuffer[T]): T = {
    val i = rng.nextInt(items.size)
    items(i)
  }

  def randNat(bound: Int): Int =
    rng.nextInt(bound)
}
