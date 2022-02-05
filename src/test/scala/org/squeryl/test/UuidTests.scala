package org.squeryl.test

import org.squeryl._
import org.squeryl.dsl.OneToMany
import org.squeryl.framework.{DBConnector, RunTestsInsideTransaction, SchemaTester, SingleTestRun}

import java.util.UUID
import org.squeryl.test.PrimitiveTypeModeForTests._

object UuidTests {
  class UuidAsProperty extends KeyedEntity[Long] {
    val id: Long = 0
    val uuid: UUID = UUID.randomUUID
  }

  class UuidWithOption(val optionalUuid: Option[UUID])
      extends KeyedEntity[Long] {
    def this() = this(Some(UUID.randomUUID()))
    val id: Long = 0
  }

  class UuidAsId extends KeyedEntity[UUID] {
    val id: UUID = UUID.randomUUID
    lazy val foreigns: OneToMany[UuidAsForeignKey] = TestSchema.uuidOneToMany.left(this)
  }

  class UuidAsForeignKey(val foreignUuid: UUID) extends KeyedEntity[Long] {
    val id: Long = 0
  }

  object TestSchema extends Schema {
    val uuidAsProperty: Table[UuidAsProperty] = table[UuidAsProperty]()
    val uuidAsId: Table[UuidAsId] = table[UuidAsId]()
    val uuidAsForeignKey: Table[UuidAsForeignKey] = table[UuidAsForeignKey]()
    val uuidWithOption: Table[UuidWithOption] = table[UuidWithOption]()

    val uuidOneToMany: PrimitiveTypeModeForTests.OneToManyRelationImpl[UuidAsId, UuidAsForeignKey] =
      oneToManyRelation(uuidAsId, uuidAsForeignKey).via(_.id === _.foreignUuid)

    override def drop: Unit = {
      Session.cleanupResources
      super.drop
    }
  }

}

abstract class UuidTests extends SchemaTester with RunTestsInsideTransaction {
  self: DBConnector =>
  import UuidTests._

  final def schema: Schema = TestSchema

  test("UuidAsProperty") {
    import TestSchema._

    val testObject = new UuidAsProperty
    testObject.save

    testObject.uuid should equal(
      uuidAsProperty.where(_.id === testObject.id).single.uuid
    )

    testObject.uuid should equal(
      uuidAsProperty.where(_.uuid in List(testObject.uuid)).single.uuid
    )
  }

  test("UuidOptional", SingleTestRun) {
    import TestSchema._

    val testObject = new UuidWithOption(None)
    testObject.save

    val fromDb = uuidWithOption.lookup(testObject.id).get
    println(fromDb.optionalUuid)
    fromDb.optionalUuid shouldBe None

    val uuid = UUID.randomUUID()

    update(uuidWithOption)(p =>
      where(p.id === testObject.id)
        set (p.optionalUuid := Some(uuid))
    )

    uuidWithOption.lookup(testObject.id).get.optionalUuid should equal(
      Some(uuid)
    )

    update(uuidWithOption)(p =>
      where(p.id === testObject.id)
        set (p.optionalUuid := None)
    )

    uuidWithOption.lookup(testObject.id).get.optionalUuid shouldBe None
  }

  test("UuidAsId") {
    import TestSchema._

    val testObject = new UuidAsId

    testObject.save

    testObject.id shouldBe uuidAsId.where(_.id === testObject.id).single.id

    testObject.id should equal(
      uuidAsId.where(_.id in List(testObject.id)).single.id
    )

    val lookup = uuidAsId.lookup(testObject.id)
    lookup.get.id shouldBe testObject.id
  }

  test("UuidAsForeignKey") {
    import TestSchema._

    val primaryObject = new UuidAsId
    primaryObject.save

    val secondaryObject = new UuidAsForeignKey(primaryObject.id)
    uuidAsForeignKey.insert(secondaryObject)

    secondaryObject.id should equal(
      uuidAsForeignKey.where(_.id === secondaryObject.id).single.id
    )

    List(secondaryObject.id) should equal(
      primaryObject.foreigns.map(_.id).toList
    )
  }
}
