package org.squeryl.test.customtypes

import org.scalatest.matchers.should.Matchers
import org.squeryl.customtypes.CustomTypesMode._
import org.squeryl.customtypes._
import org.squeryl.dsl.{ManyToOne, OneToMany}
import org.squeryl.framework._
import org.squeryl._

abstract class TestCustomTypesMode
    extends SchemaTester
    with Matchers
    with QueryTester
    with RunTestsInsideTransaction {
  self: DBConnector =>

  val schema = new HospitalDb

  import schema._

  var sharedTestObjects: TestData = null

  override def prePopulate(): Unit = {
    sharedTestObjects = new TestData(schema)
  }

  import CustomTypesMode._

  def simpleSelect: Query[Patient] =
    from(patients)(p =>
      where(p.age > 70)
        select p
    )

  test("Queries") {
    val testObjects = sharedTestObjects
    import testObjects._

    validateQuery(
      "simpleSelect",
      simpleSelect,
      (p: Patient) => p.id.value,
      List(joseCuervo.id.value)
    )
    validateQuery(
      "simpleSelect1",
      patients.where(_.age > 70),
      (p: Patient) => p.id.value,
      List(joseCuervo.id.value)
    )
    validateQuery(
      "simpleSelect2",
      patients.where(_.age < 40),
      (p: Patient) => p.id.value,
      List(raoulEspinoza.id.value)
    )
    validateQuery(
      "simpleSelect3",
      patients.where(_.age < Some(new Age(40))),
      (p: Patient) => p.id.value,
      List(raoulEspinoza.id.value)
    )
  }

  test("OneToMany") {
    val jose = patients.where(_.age > 70).single
    val pi = new PatientInfo(new Info("!!!!!"))

    val pi0 = new PatientInfo(new Info("zzzz"))

    jose.patientInfo.assign(pi0)
    jose.id.value shouldBe pi0.patientId.value
    patientInfo.insert(pi0)

    jose.patientInfo.associate(pi)

  }
}

class TestData(schema: HospitalDb) {
  val joseCuervo: Patient = schema.patients.insert(
    new Patient(
      new FirstName("Jose"),
      Some(new Age(76)),
      Some(new WeightInKilograms(290.134))
    )
  )
  val raoulEspinoza: Patient = schema.patients.insert(
    new Patient(new FirstName("Raoul"), Some(new Age(32)), None)
  )
}

object HospitalDb extends HospitalDb

class HospitalDb extends Schema {

  val patients: Table[Patient] = table[Patient]()

  val patientInfo: Table[PatientInfo] = table[PatientInfo]()

  val patientToPatientInfo: customtypes.CustomTypesMode.OneToManyRelationImpl[
    Patient,
    PatientInfo
  ] =
    oneToManyRelation(patients, patientInfo).via((p, pi) =>
      p.id === pi.patientId
    )

  override def drop: Unit = super.drop
}

class Patient(
    var firstName: FirstName,
    var age: Option[Age],
    var weight: Option[WeightInKilograms]
) extends KeyedEntity[IntField] {

  def this() = this(null, Some(new Age(1)), Some(new WeightInKilograms(1)))

  val id: IntField = null

  lazy val patientInfo: OneToMany[PatientInfo] =
    HospitalDb.patientToPatientInfo.left(this)
}

class PatientInfo(val info: Info) extends KeyedEntity[IntField] {

  def this() = this(new Info(""))

  val patientId: IntField = null

  val id: IntField = null

  lazy val patient: ManyToOne[Patient] =
    HospitalDb.patientToPatientInfo.right(this)
}

/** En example of trait that can be added to custom types, to add meta data and
  * validation
  */
trait Domain[A] {
  self: Product1[Any] =>

  def label: String

  def validate(a: A): Unit

  def value: A

  validate(value)
}

class Age(v: Int) extends IntField(v) with Domain[Int] {
  // secondary constructor to show  #93
  def this(s: String) = this(s.toInt)

  def validate(a: Int): Unit = assert(a > 0, "age must be positive, got " + a)

  def label = "age"
}

class FirstName(v: String) extends StringField(v) with Domain[String] {
  def validate(s: String): Unit =
    assert(s.length <= 50, "first name is waaaay to long : " + s)

  def label = "first name"
}

class WeightInKilograms(v: Double) extends DoubleField(v) with Domain[Double] {
  def validate(d: Double): Unit =
    assert(d > 0, "weight must be positive, got " + d)

  def label = "weight (in kilograms)"
}

class ReasonOfVisit(v: String) extends StringField(v) with Domain[String] {
  def validate(s: String): Unit =
    assert(s.length > 1, "invalid visit reason : " + s)

  def label = "reason of visit"
}

class Info(v: String) extends StringField(v) with Domain[String] {
  def validate(s: String): Unit = {}

  def label = "info"
}
