package org.squeryl.logging

class StatementInvocationEvent(
    _definitionOrCallSite: StackTraceElement,
    val start: Long,
    val end: Long,
    val rowCount: Int,
    val jdbcStatement: String
) {

  val uuid = {
    val tmp = java.util.UUID.randomUUID
    java.lang.Long.toHexString(tmp.getMostSignificantBits) + "-" +
      java.lang.Long.toHexString(tmp.getLeastSignificantBits)
  }

  def definitionOrCallSite =
    _definitionOrCallSite.toString
}

trait StatisticsListener {

  def queryExecuted(se: StatementInvocationEvent): Unit

  def resultSetIterationEnded(
      statementInvocationId: String,
      iterationEndTime: Long,
      rowCount: Int,
      iterationCompleted: Boolean
  ): Unit

  def updateExecuted(se: StatementInvocationEvent): Unit

  def insertExecuted(se: StatementInvocationEvent): Unit

  def deleteExecuted(se: StatementInvocationEvent): Unit
}

object StackMarker {

  def lastSquerylStackFrame[A](a: => A) = a
}
