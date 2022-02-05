package org.squeryl.test.schooldb

import java.sql.SQLException
import org.squeryl.annotations.Column
import org.squeryl.framework._

import java.util.Date
import java.text.SimpleDateFormat
import org.squeryl.dsl._
import org.squeryl._
import adapters.{DerbyAdapter, MSSQLServer, OracleAdapter}
import internals.{FieldMetaData, FieldReferenceLinker}

import collection.mutable.ArrayBuffer
import org.squeryl.dsl.ast.ExpressionNode
import org.squeryl.test.schooldb

object AppSpecificTypeMode extends org.squeryl.PrimitiveTypeMode {
  implicit object personKED extends KeyedEntityDef[Student, Int] {
    def getId(a: Student): Int = a.id
    def isPersisted(a: Student): Boolean = a.id > 0
    def idPropertyName = "id"
  }

  implicit object schoolDbObjectKED
      extends KeyedEntityDef[SchoolDbObject, Int] {
    def getId(a: SchoolDbObject): Int = a.id
    def isPersisted(a: SchoolDbObject): Boolean = a.id > 0
    def idPropertyName = "id"
  }

  implicit object courseKED extends KeyedEntityDef[Course, Int] {
    def getId(a: Course): Int = a.id
    def isPersisted(a: Course): Boolean = a.id > 0
    def idPropertyName = "id"
    override def optimisticCounterPropertyName: Option[String] = Some("occVersionNumber")
  }

  implicit object course2KED extends KeyedEntityDef[Course2, Int] {
    def getId(a: Course2): Int = a.id
    def isPersisted(a: Course2): Boolean = a.id > 0
    def idPropertyName = "id"
    override def optimisticCounterPropertyName: Option[String] = Some("occVersionNumber")
  }

  implicit object courseOfferingKED
      extends KeyedEntityDef[CourseOffering, CompositeKey3[Int, Long, Int]] {
    def getId(a: CourseOffering): CompositeKey3[Int, Long, Int] = a.id
    def isPersisted(a: CourseOffering): Boolean = a.isPersisted
    def idPropertyName = "id"
  }
}

import AppSpecificTypeMode._

object SingleTestRun extends org.scalatest.Tag("SingleTestRun")

class SchoolDbObject {
  val id: Int = 0
}

trait Person

class Student(
    var name: String,
    var lastName: String,
    var age: Option[Int],
    var gender: Int,
    var addressId: Option[Int],
    var isMultilingual: Option[Boolean]
) extends Person {

  val id: Int = 0

  override def toString: String = "Student:" + id + ":" + name

  def dummyKey: CompositeKey2[Option[Int], Option[Int]] = compositeKey(age, addressId)
}

case class Course2(
    id: Int,
    name: String,
    confirmed: Boolean,
    occVersionNumber: Int
)

case class Course(
    var name: String,
    var startDate: Date,
    var finalExamDate: Option[Date],
    @Column("meaninglessLongZ")
    var meaninglessLong: Long,
    @Column("meaninglessLongOption")
    var meaninglessLongOption: Option[Long],
    confirmed: Boolean
) {

  val id: Int = 0

  val occVersionNumber: Int = 0

  def occVersionNumberZ: Int = occVersionNumber

  override def toString: String = "Course:" + id + ":" + name

  val rawData: Array[Byte] = {
    val a = new Array[Byte](1)
    a(0) = 5
    a
  }
}

class CourseSubscription(var courseId: Int, var studentId: Int)
    extends SchoolDbObject {

  override def toString: String = "CourseSubscription:" + id
}

class CourseAssignment(var courseId: Int, var professorId: Long)
    extends SchoolDbObject {

  override def toString: String = "CourseAssignment:" + id
}

class Address(
    var streetName: String,
    var numberz: Int,
    var numberSuffix: Option[String],
    var appNumber: Option[Int],
    var appNumberSuffix: Option[String]
) extends SchoolDbObject {

  override def toString: String = "rue " + streetName
}

class Professor(
    var lastName: String,
    var yearlySalary: Float,
    var weight: Option[Float],
    var yearlySalaryBD: BigDecimal,
    var weightInBD: Option[BigDecimal]
) extends KeyedEntity[Long]
    with Person {

  def this() = this("", 0f, Some(0f), BigDecimal(0), Some(BigDecimal(0)))
  var id: Long = 0
  override def toString: String = "Professor:" + id + ",sal=" + yearlySalary
}

case class CourseOffering(
    courseId: Int,
    professorId: Long,
    addressId: Int,
    description: String
) extends PersistenceStatus {
  def id: CompositeKey3[Int, Long, Int] = CompositeKey3(courseId, professorId, addressId)
}

case class PostalCode(code: String) extends KeyedEntity[String] {
  def id: String = code
}

case class School(
    addressId: Int,
    name: String,
    parentSchoolId: Long,
    transientField: String
) extends KeyedEntity[Long] {
  val id_field: Long = 0

  def id: Long = id_field
}

case class SqlDate(id: Long, aDate: java.sql.Date) extends KeyedEntity[Long] {

  def this() = this(0L, new java.sql.Date(0))

}

case class YieldInspectionTest(id: Int, num: Int)

case class YieldInspectionAnother(id: Int, name: String, testId: Int)

object SDB extends SchoolDb

object Tempo extends Enumeration {
  type Tempo = Value
  val Largo: schooldb.Tempo.Value = Value(1, "Largo")
  val Allegro: schooldb.Tempo.Value = Value(2, "Allegro")
  val Presto: schooldb.Tempo.Value = Value(3, "Presto")
}

class StringKeyedEntity(val id: String, val tempo: Tempo.Tempo)
    extends KeyedEntity[String] {
  def this() = this("", Tempo.Largo)
}

class SchoolDb extends Schema {

  val courses2: Table[Course2] = table[Course2]()

  override val name: Option[String] = None

  override def columnNameFromPropertyName(n: String): String =
    NamingConventionTransforms.snakify(n)

  /** Let's illustrate the support for crappy table naming convention !
    */
  override def tableNameFromClassName(n: String): String =
    "T_" + n

  val stringKeyedEntities: Table[StringKeyedEntity] = table[StringKeyedEntity]()

  val professors: Table[Professor] = table[Professor]()

  val students: Table[Student] = table[Student]()

  val addresses: Table[Address] = table[Address]("AddressexageratelyLongName")

  val courses: Table[Course] = table[Course]()

  val courseSubscriptions: Table[CourseSubscription] = table[CourseSubscription]()

  val courseAssignments: Table[CourseAssignment] = table[CourseAssignment]()

  val courseOfferings: Table[CourseOffering] = table[CourseOffering]()

  val schools: Table[School] = table[School]()

  val postalCodes: Table[PostalCode] = table[PostalCode]()

  val tests: Table[YieldInspectionTest] = table[YieldInspectionTest]()

  val others: Table[YieldInspectionAnother] = table[YieldInspectionAnother]()

  val sqlDates: Table[SqlDate] = table[SqlDate]()

// uncomment to test : when http://www.assembla.com/spaces/squeryl/tickets/14-assertion-fails-on-self-referring-onetomanyrelationship
//  an unverted constraint gets created, unless expr. is inverted : child.parentSchoolId === parent.id
//  val schoolHierarchy =
//    oneToManyRelation(schools, schools).via((parent, child) => parent.id === child.parentSchoolId)

  on(schools)(s =>
    declare(
      s.id_field is primaryKey,
      s.name is (indexed("uniqueIndexName"), unique),
      s.name defaultsTo "no name",
      columns(s.name, s.addressId) are indexed,
      s.parentSchoolId is (indexed, unique)
    )
  )

  on(professors)(p =>
    declare(
      p.lastName is named("theLastName")
    )
  )

  on(professors)(p =>
    declare(
      p.yearlySalary is dbType("real")
    )
  )

  on(stringKeyedEntities)(e =>
    declare(
      e.tempo.defaultsTo(Tempo.Largo)
    )
  )

  on(schools)(s =>
    declare(
      s.transientField is transient
    )
  )

  // disable the override, since the above is good for Oracle only, this is not a usage demo, but
  // a necessary hack to test the dbType override mechanism and still allow the test suite can run on all database :
  override def columnTypeFor(fieldMetaData: FieldMetaData, owner: Table[_]): Option[String] =
    if (
      fieldMetaData.nameOfProperty == "yearlySalary" && Session.currentSession.databaseAdapter
        .isInstanceOf[OracleAdapter]
    )
      Some("float")
    else
      None

  override def drop: Unit = {
    Session.cleanupResources
    super.drop
  }

  def studentTransform(s: Student): Student = {
    new Student(
      s.name,
      s.lastName,
      s.age,
      (s.gender % 2) + 1,
      s.addressId,
      s.isMultilingual
    )
  }

  val beforeInsertsOfPerson = new ArrayBuffer[Person]
  val transformedStudents = new ArrayBuffer[Student]
  val beforeInsertsOfKeyedEntity = new ArrayBuffer[KeyedEntity[_]]
  val beforeInsertsOfProfessor = new ArrayBuffer[Professor]
  val afterSelectsOfStudent = new ArrayBuffer[Student]
  val afterInsertsOfProfessor = new ArrayBuffer[Professor]
  val afterInsertsOfSchool = new ArrayBuffer[School]
  val beforeDeleteOfSchool = new ArrayBuffer[School]
  val afterDeleteOfSchool = new ArrayBuffer[School]
  // will contain the identityHashCode :
  val professorsCreatedWithFactory = new ArrayBuffer[Int]

  override def callbacks = Seq(
    // We'll change the gender of z1 z2 student
    beforeInsert[Student]()
      map (s => {
        if (s.name == "z1" && s.lastName == "z2") {
          val s2 = studentTransform(s); transformedStudents.append(s2); s2
        } else s
      }),
    beforeInsert[Person]()
      map (p => { beforeInsertsOfPerson.append(p); p }),
    beforeInsert[Professor]()
      call (beforeInsertsOfProfessor.append(_)),
    beforeInsert[KeyedEntity[_]]()
      call (beforeInsertsOfKeyedEntity.append(_)),
    afterSelect[Student]()
      call (afterSelectsOfStudent.append(_)),
    afterInsert[Professor]()
      call (afterInsertsOfProfessor.append(_)),
    afterInsert(schools)
      call (afterInsertsOfSchool.append(_)),
    beforeDelete(schools) call (beforeDeleteOfSchool.append(_)),
    afterDelete(schools) call (afterDeleteOfSchool.append(_)),
    factoryFor(professors) is {
      val p = new Professor(
        "Prof From Factory !",
        80.0f,
        Some(70.5f),
        80.0f,
        Some(70.5f)
      )
      professorsCreatedWithFactory.append(System.identityHashCode(p))
      p
    }
  )

}

class TestInstance(schema: SchoolDb) {
  import schema._
  val oneHutchissonStreet: Address =
    addresses.insert(new Address("Hutchisson", 1, None, None, None))
  val twoHutchissonStreet: Address =
    addresses.insert(new Address("Hutchisson", 2, None, None, None))
  val oneTwoThreePieIXStreet: Address =
    addresses.insert(new Address("Pie IX", 123, None, Some(4), Some("A")))

  val xiao: Student = students.insert(
    new Student(
      "Xiao",
      "Jimbao Gallois",
      Some(24),
      2,
      Some(oneHutchissonStreet.id),
      Some(true)
    )
  )
  val georgi: Student = students.insert(
    new Student(
      "Georgi",
      "Balanchivadze Fourrier",
      Some(52),
      1,
      Some(oneHutchissonStreet.id),
      None
    )
  )
  val pratap: Student = students.insert(
    new Student(
      "Pratap",
      "Jamsetji Bach",
      Some(25),
      1,
      Some(oneTwoThreePieIXStreet.id),
      None
    )
  )
  val gontran: Student = students.insert(
    new Student(
      "Gontran",
      "Plourde",
      Some(25),
      1,
      Some(oneHutchissonStreet.id),
      Some(true)
    )
  )
  val gaitan: Student = students.insert(
    new Student("Gaitan", "Plouffe", Some(19), 1, None, Some(true))
  )

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val jan2009: Date = dateFormat.parse("2009-01-01")
  val may2009: Date = dateFormat.parse("2009-05-01")
  val feb2009: Date = dateFormat.parse("2009-02-01")
  val feb2010: Date = dateFormat.parse("2010-02-01")
  val feb2011: Date = dateFormat.parse("2011-02-01")

  val groupTheory: Course = courses.insert(
    Course("Group Theory", jan2009, Some(may2009), 0, None, confirmed = false)
  )
  val heatTransfer: Course = courses.insert(
    Course("Heat Transfer", feb2009, None, 3, Some(1234), confirmed = false)
  )
  val counterpoint: Course = courses.insert(
    Course("Counterpoint", feb2010, None, 0, None, confirmed = true)
  )
  val mandarin: Course = courses.insert(
    Course("Mandarin 101", feb2010, None, 0, None, confirmed = true)
  )

  courseSubscriptions.insert(new CourseSubscription(groupTheory.id, xiao.id))
  courseSubscriptions.insert(
    new CourseSubscription(heatTransfer.id, gontran.id)
  )
  courseSubscriptions.insert(new CourseSubscription(heatTransfer.id, georgi.id))
  courseSubscriptions.insert(new CourseSubscription(counterpoint.id, pratap.id))
  courseSubscriptions.insert(new CourseSubscription(mandarin.id, gaitan.id))

  val tournesol: Professor = professors.insert(
    new Professor("tournesol", 80.0f, Some(70.5f), 80.0f, Some(70.5f))
  )

  val offering1: CourseOffering = courseOfferings.insert(
    CourseOffering(
      groupTheory.id,
      tournesol.id,
      oneHutchissonStreet.id,
      "Offered Daily"
    )
  )
  val offering2: CourseOffering = courseOfferings.insert(
    CourseOffering(
      groupTheory.id,
      tournesol.id,
      twoHutchissonStreet.id,
      "May be cancelled"
    )
  )

}

abstract class FullOuterJoinTests extends SchoolDbTestBase {
  self: DBConnector =>

  import schema._

  test("NewLeftOuterJoin1Reverse") {
    val testInstance = sharedTestInstance; import testInstance._

    // loggerOn

    val leftOuterJoinStudentAddresses =
      join(addresses.leftOuter, students)((a, s) =>
        select((s, a))
          orderBy s.id
          on (s.addressId === a.map(_.id))
      )

    val res =
      (for (t <- leftOuterJoinStudentAddresses)
        yield (t._1.id, t._2.map(a => a.id))).toList

    val expected = List(
      (xiao.id, Some(oneHutchissonStreet.id)),
      (georgi.id, Some(oneHutchissonStreet.id)),
      (pratap.id, Some(oneTwoThreePieIXStreet.id)),
      (gontran.id, Some(oneHutchissonStreet.id)),
      (gaitan.id, None)
    )

    expected shouldBe res
  }
}

abstract class SchoolDbTestBase
    extends SchemaTester
    with QueryTester
    with RunTestsInsideTransaction {
  self: DBConnector =>

  lazy val schema = new SchoolDb

  var sharedTestInstance: TestInstance = null

  override def prePopulate(): Unit = {
    sharedTestInstance = new TestInstance(schema)
  }

}

abstract class CommonTableExpressions extends SchoolDbTestBase {
  self: DBConnector =>

  import schema._

  test("commonTableExpressions") {
    val qStudents = from(students)(s =>
      where(s.name === "Xiao")
        select s
    )
    val qAddresses = from(addresses)(a => select(a))

    val q =
      from(qStudents)(s =>
        withCte(qStudents, qAddresses)
          where (exists(
            join(qStudents, qStudents)((s2, s3) =>
              where(
                s2.name === "Xiao" and exists(
                  from(qStudents)(s4 => where(s4.name === "Xiao").select(s4))
                )
              )
                select (s2)
                on (s2.name === s3.name)
            )
          ) and s.name === "Xiao")
          select s
      )

    val res = for (s <- q) yield s.name
    val expected = List("Xiao")

    expected shouldBe res
  }
}

abstract class SchoolDbTestRun extends SchoolDbTestBase {
  self: DBConnector =>

  import schema._

  test("cast") {
    val q =
      from(addresses)(a => where(a.id === "1".cast[Int, TInt]("int4")) select a)
    q.toList.size shouldBe 1
  }

  test("DecimalNull", SingleTestRun) {
    val p = new Professor("Mad Professor", 80.0f, Some(70.5f), 80.0f, None)

    professors.insert(p)

    professors.lookup(p.id)
  }

  test("StringKeyedEntities") {
    stringKeyedEntities.insert(new StringKeyedEntity("123", Tempo.Largo))
  }

  test("EqualCountInSubQuery") {
    val q =
      from(courses)(c =>
        where(
          1 === from(courseSubscriptions)(cs =>
            where(c.id === cs.courseId) compute (countDistinct(cs.courseId))
          )
        )
          select c
      ).toList

    q.size shouldBe 4
  }

  test("CountSignatures") {
    val q =
      from(courseSubscriptions)(cs => compute(countDistinct(cs.courseId)))

    // 4L shouldBe q: Long
    (q: Long) shouldBe 4L

    val q2 =
      from(courseSubscriptions)(cs => compute(count(cs.courseId)))

    // 5L shouldBe q2: Long
    (q2: Long) shouldBe 5L

    val q3 =
      from(courseSubscriptions)(cs => compute(count))

    // 5L shouldBe q3: Long
    (q3: Long) shouldBe 5L

    // passed('testCountSignatures)
  }

  def avgStudentAge(): Query[Measures[Option[Float]]] =
    from(students)(s => compute(avg(s.age)))

  def avgStudentAgeFunky(): Query[Measures[Product4[Option[Float], Option[Float], Option[Double], Long]]] =
    from(students)(s =>
      compute(avg(s.age), avg(s.age) + 3, avg(s.age) / count, count + 6)
    )

  def addressesOfStudentsOlderThan24: Query[Option[String]] =
    from(students, addresses)((s, a) =>
      where((24 lt s.age) and (24 lt s.age))
        select (&(a.numberz || " " || a.streetName || " " || a.appNumber))
    )

  test("DeepNest1") {
    val testInstance = sharedTestInstance; import testInstance._

    val q = from(professors)(p0 => select(p0))

    val q1 = from(q)(p => where(p.lastName === tournesol.lastName) select (p))

    val profTournesol = q1.single

    tournesol.id shouldBe profTournesol.id
  }

  test("KeyedEntityIdRenaming") {
    postalCodes.insert(PostalCode("J0B-2C0"))
  }

  test("update to null") {
    val testInstance = sharedTestInstance; import testInstance._

    val rejan = students.insert(
      new Student(
        "Réjean",
        "Plourde",
        Some(24),
        2,
        Some(oneHutchissonStreet.id),
        Some(true)
      )
    )

    update(students)(p =>
      where(p.id === rejan.id)
        set (p.isMultilingual := None)
    )
  }

  test("DeepNest2") {
    val testInstance = sharedTestInstance; import testInstance._

    val q =
      from(from(from(professors)(p0 => select(p0)))(p1 => select(p1)))(p2 =>
        select(p2)
      )

    val q1 = from(q)(p => where(p.lastName === tournesol.lastName) select (p))

    val profTournesol = q1.single

    tournesol.id shouldBe profTournesol.id
  }

  test("assertColumnNameChangeWithDeclareSyntax") {
    val st = Session.currentSession.connection.createStatement()
    st.execute("select the_Last_Name from t_professor")
  }

  test("OptionStringInWhereClause") {
    val testInstance = sharedTestInstance; import testInstance._

    val q =
      from(addresses)(a => where(a.appNumberSuffix === Some("A")) select a)

    val h = q.head

    oneTwoThreePieIXStreet.id shouldBe h.id

    Some("A") shouldBe h.appNumberSuffix
  }

  test("blobTest") {
    val testInstance = sharedTestInstance; import testInstance._

    var c = courses.where(_.id === counterpoint.id).single

    c.rawData(0) shouldBe 5

    c.rawData(0) = 3

    c.update

    c = courses.where(_.id === counterpoint.id).single

    c.rawData(0) shouldBe 3

    val data = Array.fill(2)(2.toByte)
    courses.update(c => where(c.id === counterpoint.id) set (c.rawData := data))
    c = courses.where(_.id === counterpoint.id).single
    2 shouldBe c.rawData(0)
    2 shouldBe c.rawData(1)
  }

  test("nullCompoundKey") {
    courseOfferings.allRows.foreach { row =>
      val newRow = row.copy(description = "Cancelled")
      courseOfferings.update(newRow)
    }
  }

  /** POC for raw SQL "facilities"
    */
  class RawQuery(query: String, args: collection.Seq[Any]) {

    private def prep = {
      // We'll pretend we don't care about connection, statement, resultSet leaks for now ...
      val s = Session.currentSession

      val st = s.connection.prepareStatement(query)
      for (z <- args.zipWithIndex)
        st.setObject(z._2 + 1, z._1.asInstanceOf[AnyRef])
      st
    }

    import org.squeryl.internals._
    import org.squeryl.dsl.ast._

    def toSeq[A](t: Table[A]): Seq[A] = {
      val st = prep
      val resultSet = st.executeQuery
      val res = new scala.collection.mutable.ArrayBuffer[A]

      // now for mapping a query to Schema objects :
      val rm = new ResultSetMapper

      for ((fmd, i) <- t.posoMetaData.fieldsMetaData.zipWithIndex) {
        val jdbcIndex = i + 1
        val fse = new FieldSelectElement(null, fmd, rm)
        fse.prepareColumnMapper(jdbcIndex)
        fse.prepareMapper(jdbcIndex)
      }

      while (resultSet.next) {
        val v = t.give(rm, resultSet)
        res.append(v)
      }
      res.toSeq
    }

    def toTuple[A1, A2]()(implicit
        f1: TypedExpressionFactory[A1, _],
        f2: TypedExpressionFactory[A2, _]
    ): (A1, A2) = {

      val st = prep
      val rs = st.executeQuery

      if (!rs.next)
        sys.error("consider using toOptionTuple[....]")

      // let's pretend there was no shame to be had for such grotesque cheating :
      val m1 = f1.thisMapper.asInstanceOf[PrimitiveJdbcMapper[A1]]
      val m2 = f2.thisMapper.asInstanceOf[PrimitiveJdbcMapper[A2]]
      // in fact, there should be a wrapper type of TypedExpressionFactory only for primitive types
      // for use in such toTuple mapping ...

      (
        m1.convertFromJdbc(m1.extractNativeJdbcValue(rs, 1)),
        m2.convertFromJdbc(m2.extractNativeJdbcValue(rs, 2))
      )
    }
  }

  def query(q: String, a: Any*) = new RawQuery(q, a)

  test("InOpWithStringList") {
    val testInstance = sharedTestInstance; import testInstance._
    val r =
      from(students)(s =>
        where(s.name in Seq("Xiao", "Georgi"))
          select s.id
      ).toSet

    Set(xiao.id, georgi.id) shouldBe r
  }

  test("transient annotation") {

    val s = schools.insert(School(123, "EB123", 0, "transient !"))

    val s2 = schools.lookup(s.id).get

    s.id shouldBe s2.id

    assert(s2.transientField != "transient !")

  }

  test("lifecycleCallbacks") {

    beforeInsertsOfPerson.clear()
    beforeInsertsOfKeyedEntity.clear()
    beforeInsertsOfProfessor.clear()
    afterSelectsOfStudent.clear()
    afterInsertsOfProfessor.clear()
    beforeDeleteOfSchool.clear()
    professorsCreatedWithFactory.clear()
    transformedStudents.clear()

    val s1 =
      students.insert(new Student("z1", "z2", Some(4), 1, Some(4), Some(true)))
    val sOpt = from(students)(s =>
      where(s.name === "z1" and s.lastName === "z2") select s
    ).headOption

    assert(sOpt.isDefined && sOpt.exists(_.gender == 2))
    assert(beforeInsertsOfPerson.contains(s1))
    assert(transformedStudents.contains(s1))
    assert(sOpt.isDefined && afterSelectsOfStudent.contains(sOpt.get))
    assert(!beforeInsertsOfKeyedEntity.contains(s1))
    assert(!beforeInsertsOfProfessor.contains(s1))
    assert(!afterInsertsOfProfessor.contains(s1))

    val s2 = schools.insert(School(0, "EB", 0, ""))

    assert(beforeInsertsOfKeyedEntity.contains(s2))
    assert(!beforeInsertsOfProfessor.contains(s2))
    assert(!afterInsertsOfProfessor.contains(s2))
    assert(afterInsertsOfSchool.contains(s2))

    schools.delete(s2.id)
    assert(beforeDeleteOfSchool.contains(s2))
    assert(afterDeleteOfSchool.contains(s2))

    val s3 = professors.insert(
      new Professor("z", 3.0f, Some(2), BigDecimal(3), Some(BigDecimal(3)))
    )

    assert(beforeInsertsOfPerson.contains(s3))
    assert(beforeInsertsOfKeyedEntity.contains(s3))
    assert(beforeInsertsOfProfessor.contains(s3))
    assert(afterInsertsOfProfessor.contains(s3))

    assert(
      professors.allRows
        .map(System.identityHashCode(_))
        .toSet == professorsCreatedWithFactory.toSet
    )
  }

  test("MetaData") {

    new Student("Xiao", "Jimbao Gallois", Some(24), 2, Some(1), None)
    val fmd =
      addresses.posoMetaData.findFieldMetaDataForProperty("appNumberSuffix")
    assert(
      fmd.get.fieldType.isAssignableFrom(classOf[String]),
      "'FieldMetaData " + fmd + " should be of type java.lang.String"
    )

    val pk = addresses.posoMetaData.primaryKey
    assert(
      pk.isDefined,
      "MetaData of addresses should have 'id' as PK : \n" + addresses.posoMetaData
    )
  }

  test("OptionAndNonOptionMixInComputeTuple") {
    val x: Product4[Option[Float], Option[Float], Option[Double], Long] =
      avgStudentAgeFunky()
  }

  test("testServerSideFunctionCall") {

    val s =
      from(students)(s =>
        where(lower(s.name) === lower("GONtran"))
          select ((&(lower(s.name)), &(upper("zaza"))))
      ).single

    "gontran" shouldBe s._1
    "ZAZA" shouldBe s._2
  }

  test("ConcatWithOptionalCols") {
    val dbAdapter = Session.currentSession.databaseAdapter
    if (
      !dbAdapter.isInstanceOf[MSSQLServer] && !dbAdapter
        .isInstanceOf[DerbyAdapter]
    ) {
      // concat doesn't work in Derby with numeric fields.
      // see: https://issues.apache.org/jira/browse/DERBY-1306

      addressesOfStudentsOlderThan24.toList
    }
  }

  test("ScalarOptionQuery") {
    avgStudentAge()
  }

  test("LikeOperator") {
    val testInstance = sharedTestInstance; import testInstance._
    val q =
      from(students)(s =>
        where(s.name like "G%")
          select s.id
          orderBy (s.name)
      )

    validateQuery(
      "testLikeOperator",
      q,
      identity[Int],
      List(gaitan.id, georgi.id, gontran.id)
    )

  }

  test("SingleOption") {
    val testInstance = sharedTestInstance; import testInstance._
    val q =
      from(students)(s =>
        where(s.name like "G%")
          select s.id
          orderBy (s.name)
      )

    val shouldBeRight =
      try {
        Left(q.singleOption)
      } catch {
        case e: Exception => Right(e)
      }

    assert(
      shouldBeRight.isRight,
      "singleOption did not throw an exception when it should have"
    )

    val q2 =
      from(students)(s =>
        where(s.name like "Gontran")
          select s.id
          orderBy (s.name)
      )

    q2.singleOption shouldBe Some(gontran.id)
  }

  test("isNull and === None comparison") {
    val z1 =
      from(students)(s =>
        where({
          // TODO: REFACTOR Z
          s.isMultilingual === (None: Option[Boolean])
        })
          select s.id
      )

    val z2 =
      from(students)(s =>
        where({
          val a = s.isMultilingual.isNull
          a
        })
          select s.id
      )

    val r1 = z1.toSet
    val r2 = z2.toSet

    r1 shouldBe r2
  }

//  test("NotOperator"){
//    val testInstance = sharedTestInstance; import testInstance._
//    val q =
//      from(students)(s=>
//        where(not(s.name like "G%"))
//        select(s.id)
//        orderBy(s.name desc)
//      )
//
//    validateQuery('testNotOperator, q, identity[Int], List(xiao.id, pratap.id))
//  }

  test("DateTypeMapping") {
    val testInstance = sharedTestInstance; import testInstance._

    val mandarinCourse =
      courses.where(c => c.id === mandarin.id).single

    assert(
      mandarinCourse.startDate == feb2010,
      "testDateTypeMapping" + " failed, expected " + feb2010 + " got " + mandarinCourse.startDate
    )

    mandarinCourse.startDate = feb2011

    mandarinCourse.update

    val mandarinCourse2011 =
      courses.where(c => c.id === mandarin.id).single

    assert(
      mandarinCourse2011.startDate == feb2011,
      "testDateTypeMapping" + " failed, expected " + feb2011 + " got " + mandarinCourse2011.startDate
    )
  }

  test("java.sql.DateTypeMapping2") {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val origDate = new java.sql.Date(dateFormat.parse("2013-12-19").getTime)

    val aDate = sqlDates.insert(SqlDate(0L, origDate))

    val storedDate = sqlDates.lookup(aDate.id).get

    assert(
      storedDate.aDate == origDate,
      "expected " + origDate + " got " + storedDate.aDate
    )
  }

  test("DateOptionMapping") {
    val testInstance = sharedTestInstance; import testInstance._

    var groupTh =
      courses.where(c => c.id === groupTheory.id).single

    assert(
      groupTh.finalExamDate.contains(may2009),
      "testDateOptionMapping" + " failed, expected " + Some(
        may2009
      ) + " got " + groupTh.finalExamDate
    )

    // test date update :
    groupTh.finalExamDate = Some(feb2011)

    groupTh.update

    groupTh = courses.where(c => c.id === groupTheory.id).single

    assert(
      groupTh.finalExamDate.contains(feb2011),
      "testDateOptionMapping" + " failed, expected " + Some(
        feb2011
      ) + " got " + groupTh.finalExamDate
    )

    // test date update to null :

    groupTh.finalExamDate = None

    groupTh.update

    groupTh = courses.where(c => c.id === groupTheory.id).single

    assert(
      groupTh.finalExamDate.isEmpty,
      "testDateOptionMapping" + " failed, expected " + None + " got " + groupTh.finalExamDate
    )

    // test date update from None to Some :

    groupTh.finalExamDate = Some(may2009)

    groupTh.update

    groupTh = courses.where(c => c.id === groupTheory.id).single

    assert(
      groupTh.finalExamDate.contains(may2009),
      "testDateOptionMapping" + " failed, expected " + Some(
        may2009
      ) + " got " + groupTh.finalExamDate
    )
  }

  test("DateComparisonInWhereClause") {
    val testInstance = sharedTestInstance; import testInstance._

//    val feb2010 = dateFormat.parse("2010-02-01")
// ...
//    val groupTheory = courses.insert(new Course("Group Theory", jan2009, Some(may2009), 0, None, false))
//    val heatTransfer = courses.insert(new Course("Heat Transfer", feb2009, None, 3, Some(1234), false))
//    val counterpoint = courses.insert(new Course("Counterpoint", feb2010, None,0, None, true))
//    val mandarin = courses.insert(new Course("Mandarin 101", feb2010, None, 0, None, true))

    val jan2010 = dateFormat.parse("2010-01-01")
    val mar2010 = dateFormat.parse("2010-03-01")

    val mandarinAndCounterpointCourses =
      from(courses)(c =>
        where(c.startDate > jan2010 and c.startDate < mar2010)
          select c
          orderBy (List[ExpressionNode](c.startDate.asc, c.id.asc))
      ).toList

    val expected = List(counterpoint.id, mandarin.id)
    val result = mandarinAndCounterpointCourses.map(c => c.id)

    assert(
      expected == result,
      "testDateComparisonInWhereClause" + " expected " + expected + " got " + result
    )
  }

  test("DateOptionComparisonInWhereClause") {
    val testInstance = sharedTestInstance; import testInstance._
//    val jan2009 = dateFormat.parse("2009-01-01")
//...
//    val groupTheory = courses.insert(new Course("Group Theory", jan2009, Some(may2009), 0, None, false))
//    val heatTransfer = courses.insert(new Course("Heat Transfer", feb2009, None, 3, Some(1234), false))
//    val counterpoint = courses.insert(new Course("Counterpoint", feb2010, None,0, None, true))
//    val mandarin = courses.insert(new Course("Mandarin 101", feb2010, None, 0, None, true))

    val jan2008 = dateFormat.parse("2008-01-01")

    val result1 =
      from(courses)(c =>
        where(c.finalExamDate >= Option(jan2008) and c.finalExamDate.isNotNull)
          select c
          orderBy (c.finalExamDate, c.id.asc)
      ).toList.map(c => c.id)

    val result2 =
      from(courses)(c =>
        where(c.finalExamDate <= Some(jan2009))
          select c
          orderBy (c.finalExamDate, c.id.asc)
      ).toList.map(c => c.id)

    val result3 =
      from(courses)(c =>
        where(c.finalExamDate >= Some(feb2009))
          select c
          orderBy (c.finalExamDate, c.id.asc)
      ).toList.map(c => c.id)

    val expected = List(groupTheory.id)

    assert(
      expected == result1,
      "testDateOptionComparisonInWhereClause" + " expected " + expected + " got " + result1
    )

    assert(
      Nil == result2,
      "testDateOptionComparisonInWhereClause" + " expected " + expected + " got " + result2
    )

    assert(
      expected == result3,
      "testDateOptionComparisonInWhereClause" + " expected " + expected + " got " + result3
    )
  }

  test("NVLFunction") {
//    val groupTheory = courses.insert(new Course("Group Theory", jan2009, Some(may2009), 0, None, false))
//    val heatTransfer = courses.insert(new Course("Heat Transfer", feb2009, None, 3, Some(1234), false))
//    val counterpoint = courses.insert(new Course("Counterpoint", feb2010, None,0, None, true))
//    val mandarin = courses.insert(new Course("Mandarin 101", feb2010, None, 0, None, true))

    // Session.currentSession.setLogger(s => println(s))

    val result =
      from(courses)(c =>
        where(
          nvl(c.meaninglessLongOption, 3) <> 1234 and nvl(
            c.meaninglessLongOption,
            3
          ) === 3
        )
          select (&(nvl(c.meaninglessLongOption, 5)))
      ).toList: List[Long]

    val expected = List(5, 5, 5)

    assert(
      expected == result,
      "testNVLFunction" + " expected " + expected + " got " + result
    )
  }

  test("LongTypeMapping", SingleTestRun) {
    val testInstance = sharedTestInstance; import testInstance._

    var ht = courses.where(c => c.id === heatTransfer.id).single

    ht.meaninglessLong shouldBe 3
    assert(
      ht.meaninglessLongOption.contains(1234),
      "expected Some(1234), got " + ht.meaninglessLongOption
    )

    ht.meaninglessLong = -3
    ht.meaninglessLongOption = None

    ht.update

    ht = courses.where(c => c.id === heatTransfer.id).single

    ht.meaninglessLong shouldBe -3
    assert(
      ht.meaninglessLongOption.isEmpty,
      "expected None, got " + ht.meaninglessLongOption
    )

    ht.meaninglessLongOption = Some(4321)

    ht.update

    ht = courses.where(c => c.id === heatTransfer.id).single

    assert(
      ht.meaninglessLongOption.contains(4321),
      "expected Some(4321), got " + ht.meaninglessLongOption
    )

    ht.meaninglessLongOption = Some(1234)

    ht.update

    assert(
      ht.meaninglessLongOption.contains(1234),
      "expected Some(1234), got " + ht.meaninglessLongOption
    )
  }

  test("BooleanTypeMapping") {
    val testInstance = sharedTestInstance; import testInstance._

    var ht = courses.where(c => c.id === heatTransfer.id).single

    assert(!ht.confirmed, "expected false, got " + ht.confirmed)

//    ht.confirmed = true
//    courses.update(ht)

    update(courses)(c =>
      where(c.id === heatTransfer.id)
        set (c.confirmed := true)
    )

    ht = courses.where(c => c.id === heatTransfer.id).single
    assert(ht.confirmed, "expected true, got " + ht.confirmed)

//    ht.confirmed = false
//    courses.update(ht)

    update(courses)(c =>
      where(c.id === heatTransfer.id)
        set (c.confirmed := false)
    )

    ht = courses.where(c => c.id === heatTransfer.id).single

    assert(!ht.confirmed, "expected false, got " + ht.confirmed)
  }

  test("BooleanOptionMapping") {
    val testInstance = sharedTestInstance; import testInstance._

    var g = students.where(s => s.id === gontran.id).single

    assert(g.isMultilingual.get, "expected Some(true), got " + g.isMultilingual)

    g.isMultilingual = None
    g.update
    g = students.where(s => s.id === gontran.id).single
    assert(g.isMultilingual.isEmpty, "expected None, got " + g.isMultilingual)

    g.isMultilingual = Some(false)
    g.update
    g = students.where(s => s.id === gontran.id).single
    assert(
      !g.isMultilingual.get,
      "expected Some(false), got " + g.isMultilingual
    )

    g.isMultilingual = Some(true)
    g.update
    g = students.where(s => s.id === gontran.id).single
    assert(g.isMultilingual.get, "expected Some(true), got " + g.isMultilingual)
  }

  test("FloatType") {
    val testInstance = sharedTestInstance; import testInstance._

    var t = professors.where(p => p.id === tournesol.id).single

    t.yearlySalary shouldBe 80.0
    assert(t.weight.contains(70.5), "expected Some(70.5), got " + t.weight)

    t.yearlySalary = 90.5f
    t.weight = Some(75.7f)
    t.update
    t = professors.where(p => p.id === tournesol.id).single
    t.yearlySalary shouldBe 90.5
    assert(t.weight.contains(75.7f), "expected Some(75.7), got " + t.weight)

    t.weight = None
    t.update
    t = professors.where(p => p.id === tournesol.id).single
    assert(t.weight.isEmpty, "expected None, got " + t.weight)

    t.yearlySalary = 80.0f
    t.weight = Some(70.5f)
    professors.update(t)
    t = professors.where(p => p.id === tournesol.id).single
    t.yearlySalary shouldBe 80.0
    assert(t.weight.contains(70.5), "expected Some(70.5), got " + t.weight)
  }

  test("ForUpdate") {
    val testInstance = sharedTestInstance; import testInstance._
    val t = professors.where(p => p.id === tournesol.id).forUpdate.single

    t.yearlySalary shouldBe 80.0
    assert(t.weight.contains(70.5), "expected Some(70.5), got " + t.weight)
  }

  test("PaginatedForUpdate") {
    val testInstance = sharedTestInstance; import testInstance._
    val t =
      professors.where(p => p.id === tournesol.id).page(0, 1).forUpdate.single

    t.yearlySalary shouldBe 80.0
    assert(t.weight.contains(70.5), "expected Some(70.5), got " + t.weight)
  }

  test("PartialUpdate1") {
    val testInstance = sharedTestInstance; import testInstance._

    val initialHT = courses.where(c => c.id === heatTransfer.id).single

    val q =
      from(courses)(c =>
        select((c.id, c.meaninglessLong, c.meaninglessLongOption))
          orderBy (c.id)
      )

    val b4 = q.toList

    var nRows = courses.update(c =>
      where(c.id gt -1)
        set (c.meaninglessLong := 123L,
        c.meaninglessLongOption := c.meaninglessLongOption + 456L)
    // when meaninglessLongOption is null,the SQL addition will have a null result
    )

    val expectedAfter =
      List((1, 123, None), (2, 123, Some(1690)), (3, 123, None), (4, 123, None))
    val after = q.toList

    nRows shouldBe 4
    assert(
      expectedAfter == after,
      "expected " + expectedAfter + " got " + after
    )

    // alternative syntax :
    nRows = update(courses)(c =>
      where(c.id gt -1)
        set (c.meaninglessLong := 0L,
        c.meaninglessLongOption := c.meaninglessLongOption - 456L)
    )

    nRows shouldBe 4

    courses.forceUpdate(initialHT)

    val afterReset = q.toList

    b4 shouldBe afterReset
  }

  test("PartialUpdateWithInclusionOperator ") {

    update(courses)(c =>
      where(c.id in from(courses)(c0 => where(c0.id lt -1) select (c0.id)))
        set (c.meaninglessLong := 0L,
        c.meaninglessLongOption := c.meaninglessLongOption - 456L)
    )
  }

  test("HavingClause") {
    // The query here doesn't make much sense, we just test that valid SQL gets generated :
    val q =
      from(professors)(p =>
        groupBy(p.id, p.yearlySalary)
          having (p.yearlySalary gt 75.0f)
      )

    assert(q.statement.indexOf("Having") != -1)
    q.toList
  }

  test("HavingClause2") {
    // The query here doesn't make much sense, we just test that valid SQL gets generated :
    val q =
      from(professors)(p => {
        val v1 = groupBy(p.id, p.yearlySalary)

        val v2 = v1.having(p.yearlySalary gt 75.0f)

        val v3 = v2.compute(avg(p.yearlySalary))

        v3
      })
    q.toList

    assert(q.statement.indexOf("Having") != -1)
  }

  test("PartialUpdateWithSubQueryInSetClause") {
    val testInstance = sharedTestInstance; import testInstance._

    val zarnitsyn = professors.insert(
      new Professor("zarnitsyn", 60.0f, Some(70.5f), 60.0f, Some(70.5f))
    )

    professors.where(p => p.id === tournesol.id).single.yearlySalary

    val expected: Float = from(professors)(p0 =>
      where(tournesol.id === p0.id or p0.id === zarnitsyn.id) compute (nvl(
        avg(p0.yearlySalary),
        123
      ))
    )

    update(professors)(p =>
      where(p.id === tournesol.id)
        set (p.yearlySalary := from(professors)(p0 =>
          where(p.id === p0.id or p0.id === zarnitsyn.id) compute (nvl(
            avg(p0.yearlySalary),
            123
          ))
        ))
    )

    val after = professors.where(p => p.id === tournesol.id).single.yearlySalary

    expected shouldBe after

    update(professors)(p =>
      where(p.id === tournesol.id)
        set (p.yearlySalary := 80.0f)
    )

    professors.delete(zarnitsyn.id)
  }

  test("OptimisticCC1") {
    val testInstance = sharedTestInstance; import testInstance._

    Session.currentSession.connection.commit // we commit to release all locks

    val ht = courses.where(c => c.id === heatTransfer.id).single

    transaction {
      val ht2 = courses.where(c => c.id === heatTransfer.id).single
      ht2.update
    }

    var ex: Option[StaleUpdateException] = None
    try {
      ht.update
    } catch {
      case e: StaleUpdateException => ex = Some(e)
    }

    ex.getOrElse(
      org.squeryl.internals.Utils.throwError(
        "StaleUpdateException should have get thrown on concurrent update test."
      )
    )

    val expectedVersionNumber = ht.occVersionNumberZ + 1

    val actualVersionNumber =
      from(courses)(c =>
        where(c.id === heatTransfer.id) select c
      ).single.occVersionNumberZ

    expectedVersionNumber shouldBe actualVersionNumber
  }

  test("BatchInserts1") {
    addresses.insert(
      List(
        new Address("St-Dominique", 14, None, None, None),
        new Address("St-Urbain", 23, None, None, None),
        new Address("Sherbrooke", 1123, None, Some(454), Some("B"))
      )
    )

    addresses.insert(
      List(
        new Address("Van Horne", 14, None, None, None)
      )
    )

    val streetNames =
      List("Van Horne", "Sherbrooke", "St-Urbain", "St-Dominique")

    val q = addresses.where(a => a.streetName in streetNames)
    q.Count.toLong shouldBe 4

    addresses.delete(q)
    q.Count.toLong shouldBe 0
  }

  test("BatchUpdate1") {
    import schema._

    addresses.insert(
      List(
        new Address("St-Dominique", 14, None, None, None),
        new Address("St-Urbain", 23, None, None, None),
        new Address("Sherbrooke", 1123, None, Some(454), Some("B"))
      )
    )

    addresses.insert(
      List(
        new Address("Van Horne", 14, None, None, None)
      )
    )

    val streetNames =
      List("Van Horne", "Sherbrooke", "St-Urbain", "St-Dominique")

    val q = addresses.where(a => a.streetName in streetNames)

    addresses.update(q.map(a => { a.streetName += "Z"; a }))

    val updatedStreetNames =
      List("Van HorneZ", "SherbrookeZ", "St-UrbainZ", "St-DominiqueZ")

    val updatedQ = addresses.where(a => a.streetName in updatedStreetNames)
    updatedQ.Count.toLong shouldBe 4

    addresses.delete(updatedQ)
    updatedQ.Count.toLong shouldBe 0
  }

  test("BatchUpdateAndInsert2") {
    import schema._

    courses2.insert(
      Seq(
        Course2(0, "Programming 101", confirmed = false, 0),
        Course2(0, "Programming 102", confirmed = false, 0)
      )
    )

    val c = courses2.where(_.name like "Programming %")
    val c0 = c.toList

    c0.size shouldBe 2
    assert(!c0.exists(_.confirmed))

    courses2.update(c0.map(_.copy(confirmed = true)))

    c.count(_.confirmed) shouldBe 2
  }

  test("BigDecimal") {
    val testInstance = sharedTestInstance; import testInstance._

    val pt = professors.where(_.yearlySalaryBD.between(75, 80))

    pt.Count.toLong shouldBe 1

    tournesol.id shouldBe pt.single.id

    val babaZula = professors.insert(
      new Professor(
        "Baba Zula",
        80.0f,
        Some(70.5f),
        80.0f,
        Some(260.1234567f: BigDecimal)
      )
    )

    update(professors)(p =>
      where(p.id === babaZula.id)
        set (p.weightInBD := Some(261.123456111: BigDecimal))
    )

    val babaZula2 =
      professors.where(_.weightInBD === Some(261.123456111: BigDecimal))

    BigDecimal(261.123456111) shouldBe babaZula2.single.weightInBD.get

    update(professors)(p =>
      where(p.id === babaZula.id)
        set (p.weightInBD := Some(261.1234561112: BigDecimal))
    )

    val babaZula3 =
      professors.where(_.weightInBD === Some(261.1234561112: BigDecimal))

    babaZula3.Count.toLong shouldBe 1

    update(professors)(p =>
      where(p.id === babaZula.id)
        set (p.weightInBD := p.weightInBD plus 10 minus 5 times 4 div 2) // FIXME: mulitiplications aren't done first
    )

    val babaZula4 =
      professors.where(_.weightInBD === Some(532.2469122224: BigDecimal))

    BigDecimal(532.2469122224) shouldBe babaZula4.single.weightInBD.get
    babaZula4.Count.toLong shouldBe 1

    update(professors)(p =>
      where(p.id === babaZula.id)
        set (p.yearlySalaryBD := p.yearlySalaryBD plus 10 minus 5 times 4 div 2) // FIXME: multiplications aren't done first
    )

    val babaZula5 = professors.where(_.yearlySalaryBD === 170)

    BigDecimal(170) shouldBe babaZula5.single.yearlySalaryBD
    babaZula5.Count.toLong shouldBe 1
  }

  test("YieldInspectionResidue") {
    from(students)(s =>
      where(s.lastName === "Jimbao Gallois").select(s.name)
    ).single

    val r = FieldReferenceLinker.takeLastAccessedFieldReference
    r shouldBe empty
  }

  test("InWithCompute") {
    val z0 =
      from(students)(s2 =>
        where(s2.age gt 0)
          compute (min(s2.age))
      )

    val q2 = (z0: Query[Measures[Option[Int]]]): Query[Option[Int]]

    val q3 =
      from(students)(s =>
        where(s.age.isNotNull and s.age.in(q2))
          select s
      )

    val res = q3.single

    5 shouldBe res.id
  }

  test("IsNotNullWithInhibition") {
    val q =
      from(students)(s =>
        where(s.id.isNull.inhibitWhen(true)) // should return all students
          select s
      )

    val allStuents = students.allRows.map(_.id).toSet
    val allStudentsQ = q.map(_.id).toSet

    allStuents shouldBe allStudentsQ

    val q2 =
      from(students)(s =>
        where(s.id.isNull.inhibitWhen(false)) // should return all students
          select s
      )

    0 shouldBe q2.size
  }

  test("NewJoin1") {
    join(students, addresses.leftOuter, addresses)((s, a1, a2) => {
      select(s, a1, a2).on(s.addressId === a1.map(_.id), s.addressId === a2.id)
    })
  }

  test("NewLeftOuterJoin1") {
    val testInstance = sharedTestInstance; import testInstance._

    // loggerOn

    val leftOuterJoinStudentAddresses =
      join(students, addresses.leftOuter)((s, a) =>
        select((s, a))
          orderBy s.id
          on (s.addressId === a.map(_.id))
      )

    val res =
      (for (t <- leftOuterJoinStudentAddresses)
        yield (t._1.id, t._2.map(a => a.id))).toList

    val expected = List(
      (xiao.id, Some(oneHutchissonStreet.id)),
      (georgi.id, Some(oneHutchissonStreet.id)),
      (pratap.id, Some(oneTwoThreePieIXStreet.id)),
      (gontran.id, Some(oneHutchissonStreet.id)),
      (gaitan.id, None)
    )

    expected shouldBe res
  }

  test(
    "#62 CompositeKey with Option members generate sql with = null instead of is null"
  ) {
    // this should not blow up :
    val q =
      students.where(_.dummyKey === (None: Option[Int], None: Option[Int]))

    q.toList
  }

  test("NewLeftOuterJoin2") {
    val testInstance = sharedTestInstance; import testInstance._

    // loggerOn

    val leftOuterJoinStudentAddresses =
      join(students, addresses.leftOuter, addresses.leftOuter)((s, a, a2) =>
        select((s, a, a2))
          orderBy s.id
          on (s.addressId === a.map(_.id), s.addressId === a2.map(_.id))
      )

    val res =
      (for (t <- leftOuterJoinStudentAddresses)
        yield (t._1.id, t._2.map(a => a.id), t._3.map(a => a.id))).toList

    val expected = List(
      (xiao.id, Some(oneHutchissonStreet.id), Some(oneHutchissonStreet.id)),
      (georgi.id, Some(oneHutchissonStreet.id), Some(oneHutchissonStreet.id)),
      (
        pratap.id,
        Some(oneTwoThreePieIXStreet.id),
        Some(oneTwoThreePieIXStreet.id)
      ),
      (gontran.id, Some(oneHutchissonStreet.id), Some(oneHutchissonStreet.id)),
      (gaitan.id, None, None)
    )

    expected shouldBe res
  }

  test("Boolean2LogicalBooleanConversion") {
    val testInstance = sharedTestInstance; import testInstance._

    val multilingualStudents =
      students.where(_.isMultilingual === Option(true)).map(_.id).toSet

    multilingualStudents shouldBe Set(xiao.id, gontran.id, gaitan.id)
  }

  test("AvgBigDecimal") {
    val avgSalary: Option[BigDecimal] =
      from(professors)(p => compute(avg(p.yearlySalaryBD)))

    val avgWeight: Option[BigDecimal] =
      from(professors)(p => compute(avg(p.weightInBD)))

    val expectedAvgSal_ = professors.allRows.map(_.yearlySalaryBD.doubleValue)

    val expectedAvgSal = expectedAvgSal_.sum / expectedAvgSal_.size

    val expectedAvgWeight_ =
      professors.allRows.map(_.weightInBD).filter(_.isDefined).map(_.get)

    val expectedAvgWeight = expectedAvgWeight_.sum / expectedAvgWeight_.size

    assert(
      (expectedAvgSal - avgSalary.get.doubleValue) < 0.01,
      "testAvgBigDecimal"
    )
    assert(
      (expectedAvgWeight - avgWeight.get.doubleValue) < 0.01,
      "testAvgBigDecimal"
    )
  }

  test("NewLeftOuterJoin3") {
    val testInstance = sharedTestInstance; import testInstance._

    // loggerOn

    val leftOuterJoinStudentAddressesAndCourseSubs =
      join(students, addresses.leftOuter, courseSubscriptions)((s, a, cs) =>
        select((s, a, cs))
          orderBy (s.id, cs.courseId)
          on (s.addressId === a.map(_.id), s.id === cs.studentId)
      )

    val res =
      (for (t <- leftOuterJoinStudentAddressesAndCourseSubs)
        yield (t._1.id, t._2.map(a => a.id), t._3.courseId)).toList

    val expected = List(
      (xiao.id, Some(oneHutchissonStreet.id), 1),
      (georgi.id, Some(oneHutchissonStreet.id), 2),
      (pratap.id, Some(oneTwoThreePieIXStreet.id), 3),
      (gontran.id, Some(oneHutchissonStreet.id), 2),
      (gaitan.id, None, 4)
    )

    expected shouldBe res
  }

  test("TestYieldInspectionLeakViaCGLIB") {
    tests.insert(
      List(
        YieldInspectionTest(1, 100),
        YieldInspectionTest(1, 500),
        YieldInspectionTest(2, 600)
      )
    )
    others.insert(
      List(
        YieldInspectionAnother(1, "One", 1),
        YieldInspectionAnother(2, "Two", 2)
      )
    )

    val group = from(tests)(t => groupBy(t.id) compute (sum(t.num)))

    join(group, others)((g, o) =>
      select(g.measures.get, o)
        on (g.key === o.testId)
    ).toList
  }

  test("Exists") {
    val studentsWithAnAddress =
      from(students)(s =>
        where(
          exists(from(addresses)(a => where(s.addressId === a.id) select a.id))
        )
          select s
      )

    val res = for (s <- studentsWithAnAddress) yield s.name
    val expected = List("Xiao", "Georgi", "Pratap", "Gontran")

    expected shouldBe res
  }

  test("NotExists") {
    val studentsWithNoAddress =
      from(students)(s =>
        where(
          notExists(
            from(addresses)(a => where(s.addressId === a.id) select a.id)
          )
        )
          select s
      )
    val res = for (s <- studentsWithNoAddress) yield s.name
    val expected = List("Gaitan")

    expected shouldBe res
  }

  test("VeryNestedExists") {
    val qStudents = from(students)(s => select(s))
    val qStudentsFromStudents = from(qStudents)(s => select(s))
    val studentsWithAnAddress =
      from(qStudentsFromStudents)(s =>
        where(
          exists(
            from(addresses)(a =>
              where(s.addressId === a.id)
                select a.id
            )
          )
        )
          select s
      )

    val res = for (s <- studentsWithAnAddress) yield s.name
    val expected = List("Xiao", "Georgi", "Pratap", "Gontran")

    expected shouldBe res
  }

  test("VeryVeryNestedExists") {
    val qStudents = from(students)(s => select(s))
    val qStudentsFromStudents = from(qStudents)(s => select(s))
    val studentsWithAnAddress =
      from(qStudentsFromStudents)(s =>
        where(
          exists(
            from(addresses)(a =>
              where(
                s.addressId in
                  (from(addresses)((a2) =>
                    where(a2.id === a.id and s.addressId === a2.id)
                      select (a2.id)
                  ))
              )
                select a.id
            )
          )
        )
          select s
      )

    val res = for (s <- studentsWithAnAddress) yield s.name
    val expected = List("Xiao", "Georgi", "Pratap", "Gontran")

    expected shouldBe res
  }

  test("selectFromExists") {
    val qStudents = from(students)(s => select(s))
    val studentsWithAnAddress =
      from(qStudents)(s =>
        where(
          exists(from(addresses)(a => where(s.addressId === a.id) select a))
        )
          select s
      )
    val qAStudentIfHeHasAnAddress =
      from(studentsWithAnAddress)(s =>
        where(s.name === "Xiao")
          select s
      )

    val res = for (s <- qAStudentIfHeHasAnAddress) yield s.name
    val expected = List("Xiao")

    expected shouldBe res
  }

  test("UpdateSetAll") {
    update(students)(s => setAll(s.age := Some(30)))

    val expected: Long = from(students)(s => compute(count))
    val is: Long = from(students)(s => where(s.age === 30) compute (count))

    expected shouldBe is
  }

  test("commonTableExpressions") {
    val qStudents = from(students)(s => select(s))
    val qAddresses = from(addresses)(a => select(a))

    val q =
      from(qStudents)(s =>
        withCte(qStudents, qAddresses)
          where (exists(
            join(qStudents, qStudents)((s2, s3) =>
              where(
                s2.name === "Xiao" and exists(
                  from(qStudents)(s4 =>
                    where(s4.name === "Xiao")
                      select (s4)
                  )
                )
              )
                select (s2)
                on (s2.name === s3.name)
            )
          ) and s.name === "Xiao")
          select s
      )

    val res = for (s <- q) yield s.name
    val expected = List("Xiao")

    expected shouldBe res
  }
}

object Issue14Schema extends Schema {
  override def columnNameFromPropertyName(n: String): String =
    NamingConventionTransforms.snakify(n)

  val professors: Table[Professor] = table[Professor]("issue14")
}

abstract class Issue14 extends DbTestBase with QueryTester {
  self: DBConnector =>

  test("Issue14") {
    try {
      transaction {
        Session.currentSession.setLogger(println(_))
        val stmt = Session.currentSession.connection.createStatement
        stmt.execute("""create table issue14 (
    yearly_Salary real not null,
    weight_In_B_D decimal(20,16),
    id number primary key not null,
    last_Name varchar2(123) not null,
    yearly_Salary_B_D decimal(20,16) not null,
    weight real
  )
""")

        val seqName = (new OracleAdapter).createSequenceName(
          Issue14Schema.professors.posoMetaData
            .findFieldMetaDataForProperty("id")
            .get
        )
        try { stmt.execute("create sequence " + seqName) }
        catch {
          case _: SQLException =>
        }
      }
      transaction {
        // The problem is that because schema.create wasn't called in this JVM instance, the schema doesn't know
        // that the id should be auto-increment until too late, so id=1 gets inserted.  Then the
        // next one knows about the sequence, so it gets nextval, which is 1, resulting in a uniqueness violation.
        val moriarty = new Professor("Moriarty", 10000000.001f, None, 100, None)
        moriarty.id = 1
        Issue14Schema.professors.insert(moriarty)
        val xavier = new Professor("Xavier", 10000000.001f, None, 100, None)
        xavier.id = 1
        Issue14Schema.professors.insert(xavier)
        for (prof <- from(Issue14Schema.professors)(p => select(p))) {
          println(prof.lastName + " : " + prof.id)
        }
      }
    } finally {
      transaction { Issue14Schema.drop }
    }
  }
}
