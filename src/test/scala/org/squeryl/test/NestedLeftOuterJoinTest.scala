package org.squeryl.test

import org.scalatest.Assertion
import org.squeryl._
import org.squeryl.test.PrimitiveTypeModeForTests._
import org.squeryl.framework._

object TestSchema extends Schema {
  val a: Table[A] = table[A]()
  val b: Table[B] = table[B]()

  override def drop: Unit = super.drop
}

class A(val id: Int, val name: String) extends KeyedEntity[Int]

class B(val id: Int, val name: String, val aId: Int) extends KeyedEntity[Int]

abstract class NestedLeftOuterJoinTest
    extends SchemaTester
    with RunTestsInsideTransaction {
  self: DBConnector =>

  def schema: Schema = TestSchema

  def testInnerJoin(): Assertion = {
    val q0 = from(TestSchema.b)(b => select(b))

    val q1 = from(TestSchema.a, q0)((a, b) => where(a.id === b.aId).select((a, b)))
    checkJoinQuery(q1)

    val q2 = join(TestSchema.a, q0)((a, b) => select(a, b).on(a.id === b.aId))
    checkJoinQuery(q2)
  }

  test("InnerJoin") {

    TestSchema.a.insert(new A(1, "a one"))

    TestSchema.b.insert(new B(1, "b one", 1))

    testInnerJoin()

    val q0 = from(TestSchema.b)(b => select(b))

    val q1 = from(TestSchema.a, q0)((a, b) =>
      where(a.id === b.aId)
        select (a, b)
    )

    checkJoinQuery(q1)

    val aQuery = join(TestSchema.a, q0.leftOuter)((a, b) =>
      select(a, b)
        on (a.id === b.map(_.aId))
    )

    checkLeftJoinQuery(aQuery)
  }

  def checkLeftJoinQuery(q: Query[(A, Option[B])]): Option[Assertion] = {
    q.headOption.map { result =>
      val (_, b) = result

      b should not equal None
    }
  }

  def checkJoinQuery(q: Query[(A, B)]): Assertion = {
    q.headOption should not equal None
  }

}
