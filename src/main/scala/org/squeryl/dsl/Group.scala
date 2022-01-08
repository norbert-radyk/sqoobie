
package org.squeryl.dsl

class Group[K](k: K) {
  def key = k
}

class Measures[M](m: M) {
  def measures = m
}

class GroupWithMeasures[K,M](k: K, m: M) {
  def key = k
  def measures = m

  override def toString = {
    val sb = new java.lang.StringBuilder
    sb.append("GroupWithMeasures[")
    sb.append("key=")
    sb.append(key)
    sb.append(",measures=")
    sb.append(measures)
    sb.append("]")
    sb.toString
  }
}

object GroupWithMeasures {
	def unapply[K, M](x: GroupWithMeasures[K, M]) = Some((x.key, x.measures))
}
