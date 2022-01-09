package org.squeryl.dsl

import ast.{LogicalBoolean, ExpressionNode, QueryExpressionNode, SelectElement}
import boilerplate._
import org.squeryl.internals.ResultSetMapper
import java.sql.ResultSet

import org.squeryl.Query

trait QueryYield[R] {

  def invokeYield(resultSetMapper: ResultSetMapper, rs: ResultSet): R

  def invokeYieldForAst(
      q: QueryExpressionNode[_],
      rsm: ResultSetMapper
  ): (Iterable[SelectElement], AnyRef)

  def queryElements: (
      Option[ExpressionNode],
      Option[ExpressionNode],
      Iterable[ExpressionNode],
      Iterable[ExpressionNode],
      Iterable[Query[_]]
  )

  private[squeryl] var joinExpressions: collection.Seq[() => LogicalBoolean] =
    Nil

  def on(lb1: => LogicalBoolean) = {
    joinExpressions = Seq(() => lb1)
    new JoinQueryYield1(this)
  }

  def on(lb1: => LogicalBoolean, lb2: => LogicalBoolean) = {
    joinExpressions = Seq(() => lb1, () => lb2)
    new JoinQueryYield2(this)
  }

  def on(
      lb1: => LogicalBoolean,
      lb2: => LogicalBoolean,
      lb3: => LogicalBoolean
  ) = {
    joinExpressions = Seq(() => lb1, () => lb2, () => lb3)
    new JoinQueryYield3(this)
  }

  def on(
      lb1: => LogicalBoolean,
      lb2: => LogicalBoolean,
      lb3: => LogicalBoolean,
      lb4: => LogicalBoolean
  ) = {
    joinExpressions = Seq(() => lb1, () => lb2, () => lb3, () => lb4)
    new JoinQueryYield4(this)
  }

  def on(
      lb1: => LogicalBoolean,
      lb2: => LogicalBoolean,
      lb3: => LogicalBoolean,
      lb4: => LogicalBoolean,
      lb5: => LogicalBoolean
  ) = {
    joinExpressions = Seq(() => lb1, () => lb2, () => lb3, () => lb4, () => lb5)
    new JoinQueryYield5(this)
  }

  def on(
      lb1: => LogicalBoolean,
      lb2: => LogicalBoolean,
      lb3: => LogicalBoolean,
      lb4: => LogicalBoolean,
      lb5: => LogicalBoolean,
      lb6: => LogicalBoolean
  ) = {
    joinExpressions =
      Seq(() => lb1, () => lb2, () => lb3, () => lb4, () => lb5, () => lb6)
    new JoinQueryYield6(this)
  }

  def on(
      lb1: => LogicalBoolean,
      lb2: => LogicalBoolean,
      lb3: => LogicalBoolean,
      lb4: => LogicalBoolean,
      lb5: => LogicalBoolean,
      lb6: => LogicalBoolean,
      lb7: => LogicalBoolean
  ) = {
    joinExpressions = Seq(
      () => lb1,
      () => lb2,
      () => lb3,
      () => lb4,
      () => lb5,
      () => lb6,
      () => lb7
    )
    new JoinQueryYield7(this)
  }

  def on(
      lb1: => LogicalBoolean,
      lb2: => LogicalBoolean,
      lb3: => LogicalBoolean,
      lb4: => LogicalBoolean,
      lb5: => LogicalBoolean,
      lb6: => LogicalBoolean,
      lb7: => LogicalBoolean,
      lb8: => LogicalBoolean
  ) = {
    joinExpressions = Seq(
      () => lb1,
      () => lb2,
      () => lb3,
      () => lb4,
      () => lb5,
      () => lb6,
      () => lb7,
      () => lb8
    )
    new JoinQueryYield8(this)
  }

  def on(
      lb1: => LogicalBoolean,
      lb2: => LogicalBoolean,
      lb3: => LogicalBoolean,
      lb4: => LogicalBoolean,
      lb5: => LogicalBoolean,
      lb6: => LogicalBoolean,
      lb7: => LogicalBoolean,
      lb8: => LogicalBoolean,
      lb9: => LogicalBoolean
  ) = {
    joinExpressions = Seq(
      () => lb1,
      () => lb2,
      () => lb3,
      () => lb4,
      () => lb5,
      () => lb6,
      () => lb7,
      () => lb8,
      () => lb9
    )
    new JoinQueryYield9(this)
  }
}
