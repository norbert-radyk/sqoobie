
package org.squeryl.dsl.ast

import org.squeryl.internals.StatementWriter

class UpdateStatement(_whereClause: Option[()=>LogicalBoolean], uas: collection.Seq[UpdateAssignment])
   extends ExpressionNode {

  val whereClause: Option[LogicalBoolean] =
    _whereClause.map(_.apply())

  override def children = whereClause.toList ++ values

  def doWrite(sw: StatementWriter) = {}

  def columns =
    uas.map(ua => ua.left)

  def values =
    uas.map(ua => ua.right)
}
