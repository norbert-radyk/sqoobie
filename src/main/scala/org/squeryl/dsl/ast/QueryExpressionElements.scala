package org.squeryl.dsl.ast

trait QueryExpressionElements extends ExpressionNode {

  var inhibitAliasOnSelectElementReference = false

  def isChild(q: QueryableExpressionNode): Boolean

  def alias: String

  def selectDistinct: Boolean

  def isForUpdate: Boolean

  def page: Option[(Int, Int)]

  def unionIsForUpdate: Boolean

  def unionPage: Option[(Int, Int)]

  def views: Iterable[QueryableExpressionNode]

  def isJoinForm: Boolean

  def subQueries: Iterable[QueryableExpressionNode]

  def tableExpressions: Iterable[QueryableExpressionNode]

  def selectList: Iterable[SelectElement]

  def whereClause: Option[ExpressionNode]

  def hasUnInhibitedWhereClause =
    whereClause match {
      case None => false
      case Some(e: ExpressionNode) =>
        if (e.inhibited) false
        else if (e.children.isEmpty) true // for constant
        else (e.children.exists(!_.inhibited))
    }

  def havingClause: Option[ExpressionNode]

  def groupByClause: Iterable[ExpressionNode]

  def orderByClause: Iterable[ExpressionNode]

  def commonTableExpressions: Iterable[QueryExpressionElements]
}
