package org.squeryl.test

import org.squeryl.framework._
import org.squeryl.{Schema, Table}

abstract class LeftJoinTest
    extends SchemaTester
    with RunTestsInsideTransaction {
  self: DBConnector =>

  import org.squeryl.test.PrimitiveTypeModeForTests._

  val schema: Schema = LeftJoinSchema

  import LeftJoinSchema._

  override def prePopulate(): Unit = {

    months.insert(Month(1, "Jan"))
    months.insert(Month(2, "Feb"))
    months.insert(Month(3, "Mar"))
    months.insert(Month(4, "Apr"))
    months.insert(Month(5, "May"))
    months.insert(Month(6, "Jun"))
    months.insert(Month(7, "Jul"))
    months.insert(Month(8, "Aug"))
    months.insert(Month(9, "Sep"))
    months.insert(Month(10, "Oct"))
    months.insert(Month(11, "Nov"))
    months.insert(Month(12, "Dec"))

    items.insert(Item(1, "i1"))

    orders.insert(Order(1, 1, 1, 20))
    orders.insert(Order(2, 1, 1, 40))
    orders.insert(Order(3, 5, 1, 15))
  }

  test("return the correct results if an inner join is used") {
    val subquery = from(orders)(o =>
      groupBy(o.monthId)
        .compute(sum(o.qty))
        .orderBy(o.monthId)
    )

    val mainQuery = join(months, subquery)((m, sq) =>
      select(m, sq.measures)
        on (m.id === sq.key)
    )

    val result = transaction { mainQuery.toList }

    result should have size 2
    result(0)._2 shouldBe Some(60)
    result(1)._2 shouldBe Some(15)
  }

  test("return the correct results if a left outer join is used") {
    val subquery = from(orders)(o =>
      groupBy(o.monthId)
        .compute(sum(o.qty))
        .orderBy(o.monthId)
    )

    val mainQuery =
      join(months, subquery.leftOuter)((m, sq) =>
        select(m, sq)
          on (m.id === sq.map(_.key))
      )

    val res = transaction {
      mainQuery
        .map(e =>
          if (e._2.isEmpty) None
          else e._2.get.measures
        )
        .toSeq
    }

    res.size shouldBe 12
    res(0) shouldBe Some(60)
    res(1) shouldBe None
    res(2) shouldBe None
    res(3) shouldBe None
    res(4) shouldBe Some(15)
    res(5) shouldBe None
    res(6) shouldBe None
    res(7) shouldBe None
    res(8) shouldBe None
    res(9) shouldBe None
    res(10) shouldBe None
    res(11) shouldBe None
  }

}

import org.squeryl.test.PrimitiveTypeModeForTests._

object LeftJoinSchema extends Schema {

  val items: Table[Item] = table[Item]("Item")
  val months: Table[Month] = table[Month]("Month")
  val orders: Table[Order] = table[Order]("Ordr")

  override def drop: Unit = super.drop
}

case class Item(id: Int, name: String)
case class Month(id: Int, name: String)
case class Order(id: Int, monthId: Int, itemId: Int, qty: Int)
