
package org.squeryl.dsl.fsm

import org.squeryl.dsl._
import ast.{LogicalBoolean, UpdateStatement, UpdateAssignment}
import boilerplate.{ComputeMeasuresSignaturesFromStartOrWhereState, ComputeMeasuresSignaturesFromGroupByState, GroupBySignatures, OrderBySignatures}

import org.squeryl.Query

abstract sealed class Conditioned
abstract sealed class Unconditioned

trait StartState
  extends GroupBySignatures
  with ComputeMeasuresSignaturesFromStartOrWhereState {
  self: QueryElements[_] =>

  def select[R](yieldClosure: =>R): SelectState[R]
}

trait QueryElements[Cond]
  extends WhereState[Cond]
    with ComputeMeasuresSignaturesFromStartOrWhereState
    with StartState {

  private [squeryl] def whereClause:Option[()=>LogicalBoolean] = None

  private [squeryl] def commonTableExpressions: List[Query[_]] = Nil
}

trait SelectState[R] extends QueryYield[R] with OrderBySignatures[R] {
  self: BaseQueryYield[R] =>
}

trait ComputeStateStartOrWhereState[M]
  extends QueryYield[Measures[M]]
    with OrderBySignatures[Measures[M]] {

  self: MeasuresQueryYield[M] =>
}

trait WhereState[Cond]
  extends GroupBySignatures 
  with ComputeMeasuresSignaturesFromStartOrWhereState {
  self: QueryElements[_] =>

  def select[R](yieldClosure: =>R): SelectState[R] =
    new BaseQueryYield[R](this, () => yieldClosure)

  def set(updateAssignments: UpdateAssignment*)(implicit cond: Cond =:= Conditioned) =
    new UpdateStatement(whereClause, updateAssignments)
  
  def setAll(updateAssignments: UpdateAssignment*)(implicit cond: Cond =:= Unconditioned) =
    new UpdateStatement(whereClause, updateAssignments)    
}

trait HavingState[G]
    extends ComputeMeasuresSignaturesFromGroupByState[G]  {
  self: GroupQueryYield[G] =>
}

trait ComputeStateFromGroupByState[K,M]
  extends QueryYield[GroupWithMeasures[K,M]] with OrderBySignatures[GroupWithMeasures[K,M]] {
  self: GroupWithMeasuresQueryYield[K,M] =>
}


trait GroupByState[K]
  extends QueryYield[Group[K]]
  with ComputeMeasuresSignaturesFromGroupByState[K]
  with OrderBySignatures[Group[K]] {
  self: GroupQueryYield[K] =>

  def having(b: => LogicalBoolean) = {
    _havingClause = Some(() => b)
    this
  }
}

class WithState(override val commonTableExpressions: List[Query[_]])
  extends WhereState[Unconditioned]
  with ComputeMeasuresSignaturesFromStartOrWhereState
  with StartState
  with QueryElements[Unconditioned] {

  def where(b: =>LogicalBoolean): WhereState[Conditioned] =
    new QueryElementsImpl[Conditioned](Some(() => b), commonTableExpressions)
}

private [squeryl] class QueryElementsImpl[Cond](
  override val whereClause: Option[()=>LogicalBoolean],
  override val commonTableExpressions: List[Query[_]])
  extends QueryElements[Cond]
