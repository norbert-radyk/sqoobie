package org.squeryl.test.arrays

import _root_.org.squeryl.framework._
import org.squeryl.test.PrimitiveTypeModeForTests._
import org.squeryl.{Schema, Table}

abstract class PrimitiveArrayTest extends SchemaTester with RunTestsInsideTransaction { self: DBConnector =>

  val schema: Schema = PrimitiveArraySchema

  import PrimitiveArraySchema._

  test(
    "can insert and query integer, double, and long array values in database"
  ) {
    transaction {
      schema.drop
      schema.create
      swimmers.insert(
        new Swimmer(
          1,
          Array(10.55, 12.99, 15.32),
          Array(100, 110, 20),
          Array(9876543210L, 123456789L),
          Array("testing", "stuff")
        )
      )
    }

    val query = from(swimmers)(s => select(s))
    val res = transaction { query.toList }

    res.size shouldBe 1
    res(0).lap_times.length shouldBe 3
    res(0).lap_times(0) shouldBe 10.55
    res(0).lap_times(1) shouldBe 12.99
    res(0).lap_times(2) shouldBe 15.32

    res(0).scores.length shouldBe 3
    res(0).scores(0) shouldBe 100
    res(0).scores(1) shouldBe 110
    res(0).scores(2) shouldBe 20

    res(0).orgids.length shouldBe 2
    res(0).orgids(0) shouldBe 9876543210L
    res(0).orgids(1) shouldBe 123456789L

    res(0).tags.length shouldBe 2
    res(0).tags(0) shouldBe "testing"
    res(0).tags(1) shouldBe "stuff"
  }
  test("can update integer, double, and long array values in database") {
    transaction {
      schema.drop
      schema.create
      swimmers.insert(
        new Swimmer(
          1,
          Array(10.55, 12.99, 15.32),
          Array(100, 110, 20),
          Array(9876543210L, 123456789L),
          Array("testing", "stuff")
        )
      )
    }

    val query = from(swimmers)(s => select(s))
    val res = transaction { query.toList }

    res.size shouldBe 1
    res(0).lap_times.length shouldBe 3
    res(0).scores.length shouldBe 3
    res(0).orgids.length shouldBe 2
    res(0).tags.length shouldBe 2

    transaction {
      update(swimmers)(s =>
        where(s.id === 1)
          set (s.lap_times := Array(11.69), s.scores := Array(
            1,
            2,
            3,
            4,
            5
          ), s.orgids := Array(13L), s.tags := Array("and things"))
      )
    }

    from(swimmers)(s => select(s))
    val res2 = transaction { query.toList }

    res2.size shouldBe 1
    res2(0).lap_times.length shouldBe 1
    res2(0).scores.length shouldBe 5
    res2(0).orgids.length shouldBe 1
    res2(0).tags.length shouldBe 1

    res2(0).lap_times(0) shouldBe 11.69
    res2(0).scores(2) shouldBe 3
    res2(0).orgids(0) shouldBe 13L
    res2(0).tags(0) shouldBe "and things"
  }
}

object PrimitiveArraySchema extends Schema {

  val swimmers: Table[Swimmer] = table[Swimmer]("swimmer")

  override def drop: Unit = super.drop
}

class Swimmer(
    val id: Int,
    val lap_times: Array[Double],
    val scores: Array[Int],
    val orgids: Array[Long],
    val tags: Array[String]
)
