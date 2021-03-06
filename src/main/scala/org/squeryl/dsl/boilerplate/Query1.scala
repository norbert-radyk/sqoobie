package org.squeryl.dsl.boilerplate

import java.sql.ResultSet
import org.squeryl.internals.ResultSetMapper
import org.squeryl.dsl.{QueryYield, AbstractQuery}
import org.squeryl.{Query, Queryable}

class Query1[T1, R](
    t1: Queryable[T1],
    f: (T1) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query1[T1, R](t1, f, root, copyUnions(unions ++ newUnions))

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(sq1.give(rs)).invokeYield(rsm, rs)

  val ast = buildAst(f(sq1.sample), sq1)

}

class Query2[T1, T2, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    f: (T1, T2) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query2[T1, T2, R](t1, t2, f, root, copyUnions(unions ++ newUnions))

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(sq1.give(rs), sq2.give(rs)).invokeYield(rsm, rs)

  val ast = buildAst(f(sq1.sample, sq2.sample), sq1, sq2)
}

class Query3[T1, T2, T3, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    f: (T1, T2, T3) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query3[T1, T2, T3, R](
      t1,
      t2,
      t3,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(sq1.give(rs), sq2.give(rs), sq3.give(rs)).invokeYield(rsm, rs)

  val ast = buildAst(f(sq1.sample, sq2.sample, sq3.sample), sq1, sq2, sq3)
}

class Query4[T1, T2, T3, T4, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    t4: Queryable[T4],
    f: (T1, T2, T3, T4) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)
  val sq4 = createSubQueryable(t4)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query4[T1, T2, T3, T4, R](
      t1,
      t2,
      t3,
      t4,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(sq1.give(rs), sq2.give(rs), sq3.give(rs), sq4.give(rs))
      .invokeYield(rsm, rs)

  val ast = buildAst(
    f(sq1.sample, sq2.sample, sq3.sample, sq4.sample),
    sq1,
    sq2,
    sq3,
    sq4
  )
}

class Query5[T1, T2, T3, T4, T5, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    t4: Queryable[T4],
    t5: Queryable[T5],
    f: (T1, T2, T3, T4, T5) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)
  val sq4 = createSubQueryable(t4)
  val sq5 = createSubQueryable(t5)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query5[T1, T2, T3, T4, T5, R](
      t1,
      t2,
      t3,
      t4,
      t5,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(sq1.give(rs), sq2.give(rs), sq3.give(rs), sq4.give(rs), sq5.give(rs))
      .invokeYield(rsm, rs)

  val ast = buildAst(
    f(sq1.sample, sq2.sample, sq3.sample, sq4.sample, sq5.sample),
    sq1,
    sq2,
    sq3,
    sq4,
    sq5
  )
}

class Query6[T1, T2, T3, T4, T5, T6, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    t4: Queryable[T4],
    t5: Queryable[T5],
    t6: Queryable[T6],
    f: (T1, T2, T3, T4, T5, T6) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)
  val sq4 = createSubQueryable(t4)
  val sq5 = createSubQueryable(t5)
  val sq6 = createSubQueryable(t6)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query6[T1, T2, T3, T4, T5, T6, R](
      t1,
      t2,
      t3,
      t4,
      t5,
      t6,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(
      sq1.give(rs),
      sq2.give(rs),
      sq3.give(rs),
      sq4.give(rs),
      sq5.give(rs),
      sq6.give(rs)
    ).invokeYield(rsm, rs)

  val ast = buildAst(
    f(sq1.sample, sq2.sample, sq3.sample, sq4.sample, sq5.sample, sq6.sample),
    sq1,
    sq2,
    sq3,
    sq4,
    sq5,
    sq6
  )
}

class Query7[T1, T2, T3, T4, T5, T6, T7, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    t4: Queryable[T4],
    t5: Queryable[T5],
    t6: Queryable[T6],
    t7: Queryable[T7],
    f: (T1, T2, T3, T4, T5, T6, T7) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)
  val sq4 = createSubQueryable(t4)
  val sq5 = createSubQueryable(t5)
  val sq6 = createSubQueryable(t6)
  val sq7 = createSubQueryable(t7)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query7[T1, T2, T3, T4, T5, T6, T7, R](
      t1,
      t2,
      t3,
      t4,
      t5,
      t6,
      t7,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(
      sq1.give(rs),
      sq2.give(rs),
      sq3.give(rs),
      sq4.give(rs),
      sq5.give(rs),
      sq6.give(rs),
      sq7.give(rs)
    ).invokeYield(rsm, rs)

  val ast = buildAst(
    f(
      sq1.sample,
      sq2.sample,
      sq3.sample,
      sq4.sample,
      sq5.sample,
      sq6.sample,
      sq7.sample
    ),
    sq1,
    sq2,
    sq3,
    sq4,
    sq5,
    sq6,
    sq7
  )
}

class Query8[T1, T2, T3, T4, T5, T6, T7, T8, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    t4: Queryable[T4],
    t5: Queryable[T5],
    t6: Queryable[T6],
    t7: Queryable[T7],
    t8: Queryable[T8],
    f: (T1, T2, T3, T4, T5, T6, T7, T8) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)
  val sq4 = createSubQueryable(t4)
  val sq5 = createSubQueryable(t5)
  val sq6 = createSubQueryable(t6)
  val sq7 = createSubQueryable(t7)
  val sq8 = createSubQueryable(t8)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query8[T1, T2, T3, T4, T5, T6, T7, T8, R](
      t1,
      t2,
      t3,
      t4,
      t5,
      t6,
      t7,
      t8,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(
      sq1.give(rs),
      sq2.give(rs),
      sq3.give(rs),
      sq4.give(rs),
      sq5.give(rs),
      sq6.give(rs),
      sq7.give(rs),
      sq8.give(rs)
    ).invokeYield(rsm, rs)

  val ast = buildAst(
    f(
      sq1.sample,
      sq2.sample,
      sq3.sample,
      sq4.sample,
      sq5.sample,
      sq6.sample,
      sq7.sample,
      sq8.sample
    ),
    sq1,
    sq2,
    sq3,
    sq4,
    sq5,
    sq6,
    sq7,
    sq8
  )
}

class Query9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    t4: Queryable[T4],
    t5: Queryable[T5],
    t6: Queryable[T6],
    t7: Queryable[T7],
    t8: Queryable[T8],
    t9: Queryable[T9],
    f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => QueryYield[R],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)
  val sq4 = createSubQueryable(t4)
  val sq5 = createSubQueryable(t5)
  val sq6 = createSubQueryable(t6)
  val sq7 = createSubQueryable(t7)
  val sq8 = createSubQueryable(t8)
  val sq9 = createSubQueryable(t9)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      t1,
      t2,
      t3,
      t4,
      t5,
      t6,
      t7,
      t8,
      t9,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(
      sq1.give(rs),
      sq2.give(rs),
      sq3.give(rs),
      sq4.give(rs),
      sq5.give(rs),
      sq6.give(rs),
      sq7.give(rs),
      sq8.give(rs),
      sq9.give(rs)
    ).invokeYield(rsm, rs)

  val ast = buildAst(
    f(
      sq1.sample,
      sq2.sample,
      sq3.sample,
      sq4.sample,
      sq5.sample,
      sq6.sample,
      sq7.sample,
      sq8.sample,
      sq9.sample
    ),
    sq1,
    sq2,
    sq3,
    sq4,
    sq5,
    sq6,
    sq7,
    sq8,
    sq9
  )
}

class Query10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](
    t1: Queryable[T1],
    t2: Queryable[T2],
    t3: Queryable[T3],
    t4: Queryable[T4],
    t5: Queryable[T5],
    t6: Queryable[T6],
    t7: Queryable[T7],
    t8: Queryable[T8],
    t9: Queryable[T9],
    t10: Queryable[T10],
    f: Function10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, QueryYield[R]],
    isRoot: Boolean,
    unions: List[(String, Query[R])]
) extends AbstractQuery[R](isRoot, unions) {

  val sq1 = createSubQueryable(t1)
  val sq2 = createSubQueryable(t2)
  val sq3 = createSubQueryable(t3)
  val sq4 = createSubQueryable(t4)
  val sq5 = createSubQueryable(t5)
  val sq6 = createSubQueryable(t6)
  val sq7 = createSubQueryable(t7)
  val sq8 = createSubQueryable(t8)
  val sq9 = createSubQueryable(t9)
  val sq10 = createSubQueryable(t10)

  def createCopy(root: Boolean, newUnions: List[(String, Query[R])]) =
    new Query10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](
      t1,
      t2,
      t3,
      t4,
      t5,
      t6,
      t7,
      t8,
      t9,
      t10,
      f,
      root,
      copyUnions(unions ++ newUnions)
    )

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): R =
    f(
      sq1.give(rs),
      sq2.give(rs),
      sq3.give(rs),
      sq4.give(rs),
      sq5.give(rs),
      sq6.give(rs),
      sq7.give(rs),
      sq8.give(rs),
      sq9.give(rs),
      sq10.give(rs)
    ).invokeYield(rsm, rs)

  val ast = buildAst(
    f(
      sq1.sample,
      sq2.sample,
      sq3.sample,
      sq4.sample,
      sq5.sample,
      sq6.sample,
      sq7.sample,
      sq8.sample,
      sq9.sample,
      sq10.sample
    ),
    sq1,
    sq2,
    sq3,
    sq4,
    sq5,
    sq6,
    sq7,
    sq8,
    sq9,
    sq10
  )
}
