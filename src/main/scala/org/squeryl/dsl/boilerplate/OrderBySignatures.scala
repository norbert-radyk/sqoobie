
package org.squeryl.dsl.boilerplate

import org.squeryl.dsl.{QueryYield}
import org.squeryl.dsl.fsm.BaseQueryYield
import org.squeryl.dsl.ast.ExpressionNode

trait OrderBySignatures[R] {
  self: BaseQueryYield[R] =>

  type O = ExpressionNode

  def orderBy(args: List[O]): QueryYield[R] = {
    _orderByExpressions = () => args.map(() => _)
    this
  }
  
  def orderBy(e1: =>O): QueryYield[R] = {
    _orderByExpressions = ()=> List(() => e1)
    this
  }

  def orderBy(e1: =>O, e2: =>O): QueryYield[R] = {
    _orderByExpressions = ()=> List(() => e1, () => e2)
    this
  }

  def orderBy(e1: =>O, e2: =>O, e3: =>O): QueryYield[R] = {
    _orderByExpressions = ()=> List(() => e1, () => e2, () => e3)
    this
  }

  def orderBy(e1: =>O, e2: =>O, e3: =>O, e4: =>O): QueryYield[R] = {
    _orderByExpressions = ()=> List(() => e1, () => e2, () => e3, () => e4)
    this
  }

  def orderBy(e1: =>O, e2: =>O, e3: =>O, e4: =>O, e5: =>O): QueryYield[R] = {
    _orderByExpressions = ()=> List(() => e1, () => e2, () => e3, () => e4, () => e5)
    this
  }

  def orderBy(e1: =>O, e2: =>O, e3: =>O, e4: =>O, e5: =>O, e6: =>O): QueryYield[R] = {
    _orderByExpressions = ()=> List(() => e1, () => e2, () => e3, () => e4, () => e5, () => e6)
    this
  }

  def orderBy(e1: =>O, e2: =>O, e3: =>O, e4: =>O, e5: =>O, e6: =>O, e7: =>O): QueryYield[R] = {
    _orderByExpressions = ()=> List(() => e1, () => e2, () => e3, () => e4, () => e5, () => e6, () => e7)
    this
  }
}
