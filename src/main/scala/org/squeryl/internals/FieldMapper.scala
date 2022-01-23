package org.squeryl.internals

import org.squeryl.dsl._

import java.sql
import java.sql.{ResultSet, Timestamp}
import java.util.{Date, UUID}
import scala.annotation.tailrec
import scala.collection.mutable

trait FieldMapper {

  private[this] val registry = new mutable.HashMap[Class[_], FieldAttributesBasedOnType[_]]

  implicit def thisFieldMapper: FieldMapper = this

  protected object PrimitiveTypeSupport {

    val stringTEF: BasicTEF[String, TString] = new BasicTEF[String, TString] {
      val sample: String = ""
      val defaultColumnLength = 128
      def extractNativeJdbcValue(rs: ResultSet, i: Int): String = rs.getString(i)
    }

    val optionStringTEF: BasicOptionTEF[String, TString, TOptionString] = new BasicOptionTEF[String, TString, TOptionString] {
      val deOptionizer: TypedExpressionFactory[String, TString] with JdbcMapper[String, String] = stringTEF
    }

    val dateTEF: BasicTEF[Date, TDate] = new BasicTEF[Date, TDate] {
      val sample = new Date
      val defaultColumnLength: Int = -1
      def extractNativeJdbcValue(rs: ResultSet, i: Int): Date = rs.getDate(i)
    }

    val optionDateTEF: BasicOptionTEF[Date, TDate, TOptionDate] = new BasicOptionTEF[Date, TDate, TOptionDate] {
      val deOptionizer: TypedExpressionFactory[Date, TDate] with JdbcMapper[Date, Date] = dateTEF
    }

    val sqlDateTEF: BasicTEF[sql.Date, TDate] = new BasicTEF[sql.Date, TDate] {
      val sample = new java.sql.Date(0L)
      val defaultColumnLength: Int = -1
      def extractNativeJdbcValue(rs: ResultSet, i: Int): sql.Date = rs.getDate(i)
    }

    val optionSqlDateTEF: BasicOptionTEF[sql.Date, TDate, TOptionDate] = new BasicOptionTEF[sql.Date, TDate, TOptionDate] {
      val deOptionizer: TypedExpressionFactory[sql.Date, TDate] with JdbcMapper[sql.Date, sql.Date] = sqlDateTEF
    }

    val timestampTEF: BasicTEF[Timestamp, TTimestamp] = new BasicTEF[Timestamp, TTimestamp] {
      val sample = new Timestamp(0)
      val defaultColumnLength: Int = -1
      def extractNativeJdbcValue(rs: ResultSet, i: Int): Timestamp = rs.getTimestamp(i)
    }

    val optionTimestampTEF: BasicOptionTEF[Timestamp, TTimestamp, TOptionTimestamp] = new BasicOptionTEF[Timestamp, TTimestamp, TOptionTimestamp] {
      val deOptionizer: TypedExpressionFactory[Timestamp, TTimestamp] with JdbcMapper[Timestamp, Timestamp] = timestampTEF
    }

    val booleanTEF: BasicTEF[Boolean, TBoolean] = new BasicTEF[Boolean, TBoolean] {
      val sample = true
      val defaultColumnLength = 1
      def extractNativeJdbcValue(rs: ResultSet, i: Int): Boolean = rs.getBoolean(i)
    }

    val optionBooleanTEF: BasicOptionTEF[Boolean, TBoolean, TOptionBoolean] = new BasicOptionTEF[Boolean, TBoolean, TOptionBoolean] {
      val deOptionizer: TypedExpressionFactory[Boolean, TBoolean] with JdbcMapper[Boolean, Boolean] = booleanTEF
    }

    val uuidTEF: BasicTEF[UUID, TUUID] = new BasicTEF[UUID, TUUID] {
      val sample: UUID = java.util.UUID.fromString("00000000-0000-0000-0000-000000000000")
      val defaultColumnLength = 36
      def extractNativeJdbcValue(rs: ResultSet, i: Int): UUID = rs.getObject(i) match {
        case u: UUID   => u
        case s: String => UUID.fromString(s)
        case _         => sample
      }
    }

    val optionUUIDTEF: BasicOptionTEF[UUID, TUUID, TOptionUUID] = new BasicOptionTEF[UUID, TUUID, TOptionUUID] {
      val deOptionizer: TypedExpressionFactory[UUID, TUUID] with JdbcMapper[UUID, UUID] = uuidTEF
    }

    val binaryTEF: BasicTEF[Array[Byte], TByteArray] = new BasicTEF[Array[Byte], TByteArray] {
      val sample: Array[Byte] = Array(0: Byte)
      val defaultColumnLength = 255
      def extractNativeJdbcValue(rs: ResultSet, i: Int): Array[Byte] = rs.getBytes(i)
    }

    val optionByteArrayTEF: BasicOptionTEF[Array[Byte], TByteArray, TOptionByteArray] = new BasicOptionTEF[Array[Byte], TByteArray, TOptionByteArray] {
      val deOptionizer: TypedExpressionFactory[Array[Byte], TByteArray] with JdbcMapper[Array[Byte], Array[Byte]] = binaryTEF
    }

    val intArrayTEF: ArrayTEF[Int, TIntArray] = new ArrayTEF[Int, TIntArray] {
      val sample: Array[Int] = Array(0)
      def toWrappedJDBCType(element: Int): java.lang.Object =
        java.lang.Integer.valueOf(element)
      def fromWrappedJDBCType(elements: Array[java.lang.Object]): Array[Int] =
        elements.map(i => i.asInstanceOf[java.lang.Integer].toInt)
    }

    val longArrayTEF: ArrayTEF[Long, TLongArray] =
      new ArrayTEF[Long, TLongArray] {
        val sample: Array[Long] = Array(0L)
        def toWrappedJDBCType(element: Long): java.lang.Object =
          java.lang.Long.valueOf(element)
        def fromWrappedJDBCType(
            elements: Array[java.lang.Object]
        ): Array[Long] =
          elements.map(i => i.asInstanceOf[java.lang.Long].toLong)
      }

    val doubleArrayTEF: ArrayTEF[Double, TDoubleArray] =
      new ArrayTEF[Double, TDoubleArray] {
        val sample: Array[Double] = Array(0.0)
        def toWrappedJDBCType(element: Double): java.lang.Object =
          java.lang.Double.valueOf(element)
        def fromWrappedJDBCType(
            elements: Array[java.lang.Object]
        ): Array[Double] =
          elements.map(i => i.asInstanceOf[java.lang.Double].toDouble)
      }

    val stringArrayTEF: ArrayTEF[String, TStringArray] =
      new ArrayTEF[String, TStringArray] {
        val sample: Array[String] = Array("")
        def toWrappedJDBCType(element: String): java.lang.Object =
          new java.lang.String(element)
        def fromWrappedJDBCType(
            elements: Array[java.lang.Object]
        ): Array[String] =
          elements.map(i => i.asInstanceOf[java.lang.String])
      }

    def enumValueTEF[A >: Enumeration#Value <: Enumeration#Value](
        ev: Enumeration#Value
    ): JdbcMapper[Int, A] with TypedExpressionFactory[A, TEnumValue[A]] =
      new JdbcMapper[Int, A] with TypedExpressionFactory[A, TEnumValue[A]] {

        val enu: Enumeration = Utils.enumerationForValue(ev)

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Int = rs.getInt(i)
        def defaultColumnLength: Int = intTEF.defaultColumnLength
        def sample: A = ev
        def convertToJdbc(v: A): Int = v.id
        def convertFromJdbc(v: Int): A = {
          enu.values
            .find(_.id == v)
            .getOrElse(
              DummyEnum.DummyEnumerationValue
            ) // JDBC has no concept of null value for primitive types (ex. Int)
          // at this level, we mimic this JDBC flaw (the Option / None based on jdbc.wasNull will get sorted out by optionEnumValueTEF)
        }
      }

    object DummyEnum extends Enumeration {
      type DummyEnum = Value
      val DummyEnumerationValue: DummyEnum = Value(-1, "DummyEnumerationValue")
    }

    def optionEnumValueTEF[A >: Enumeration#Value <: Enumeration#Value](
        ev: Option[Enumeration#Value]
    ): TypedExpressionFactory[Option[A], TOptionEnumValue[A]] with DeOptionizer[Int, A, TEnumValue[A], Option[A], TOptionEnumValue[A]] =
      new TypedExpressionFactory[Option[A], TOptionEnumValue[A]]
        with DeOptionizer[
          Int,
          A,
          TEnumValue[A],
          Option[A],
          TOptionEnumValue[A]
        ] {
        val deOptionizer: TypedExpressionFactory[A, TEnumValue[A]] with JdbcMapper[Int, A] = {
          val e =
            ev.getOrElse(PrimitiveTypeSupport.DummyEnum.DummyEnumerationValue)
          enumValueTEF[A](e)
        }
      }

    // =========================== Numerical Floating Point ===========================

    val floatTEF: FloatTypedExpressionFactory[Float, TFloat] with PrimitiveJdbcMapper[Float] =
      new FloatTypedExpressionFactory[Float, TFloat] with PrimitiveJdbcMapper[Float] {
        val sample = 1f
        val defaultColumnLength = 4
        def extractNativeJdbcValue(rs: ResultSet, i: Int): Float =
          rs.getFloat(i)
      }

    val optionFloatTEF: FloatTypedExpressionFactory[Option[Float], TOptionFloat] with DeOptionizer[Float, Float, TFloat, Option[Float], TOptionFloat] =
      new FloatTypedExpressionFactory[Option[Float], TOptionFloat] with DeOptionizer[Float, Float, TFloat, Option[Float], TOptionFloat] {
        val deOptionizer: TypedExpressionFactory[Float, TFloat] with JdbcMapper[Float, Float] = floatTEF
      }

    val doubleTEF: FloatTypedExpressionFactory[Double, TDouble] with PrimitiveJdbcMapper[Double] =
      new FloatTypedExpressionFactory[Double, TDouble] with PrimitiveJdbcMapper[Double] {
        val sample = 1d
        val defaultColumnLength = 8
        def extractNativeJdbcValue(rs: ResultSet, i: Int): Double =
          rs.getDouble(i)
      }

    val optionDoubleTEF: FloatTypedExpressionFactory[Option[Double], TOptionDouble]
      with DeOptionizer[Double, Double, TDouble, Option[
        Double
      ], TOptionDouble] =
      new FloatTypedExpressionFactory[Option[Double], TOptionDouble]
        with DeOptionizer[Double, Double, TDouble, Option[
          Double
        ], TOptionDouble] {
        val deOptionizer: TypedExpressionFactory[Double, TDouble] with JdbcMapper[Double, Double] = doubleTEF
      }

    // =========================== Numerical Integral ===========================

    val byteTEF: IntegralTypedExpressionFactory[Byte, TByte, Float, TFloat] with PrimitiveJdbcMapper[Byte] =
      new IntegralTypedExpressionFactory[Byte, TByte, Float, TFloat] with PrimitiveJdbcMapper[Byte] {
        val sample: Byte = 1: Byte
        val defaultColumnLength = 1
        val floatifyer: TypedExpressionFactory[Float, TFloat] = floatTEF
        def extractNativeJdbcValue(rs: ResultSet, i: Int): Byte = rs.getByte(i)
      }

    val optionByteTEF: IntegralTypedExpressionFactory[Option[
      Byte
    ], TOptionByte, Option[Float], TOptionFloat] with DeOptionizer[Byte, Byte, TByte, Option[Byte], TOptionByte] =
      new IntegralTypedExpressionFactory[Option[
        Byte
      ], TOptionByte, Option[Float], TOptionFloat] with DeOptionizer[Byte, Byte, TByte, Option[Byte], TOptionByte] {
        val deOptionizer: TypedExpressionFactory[Byte, TByte] with JdbcMapper[Byte, Byte] =
          byteTEF
        val floatifyer: TypedExpressionFactory[Option[Float], TOptionFloat] =
          optionFloatTEF
      }

    val intTEF: IntegralTypedExpressionFactory[Int, TInt, Float, TFloat] with PrimitiveJdbcMapper[Int] =
      new IntegralTypedExpressionFactory[Int, TInt, Float, TFloat] with PrimitiveJdbcMapper[Int] {
        val sample = 1
        val defaultColumnLength = 4
        val floatifyer: TypedExpressionFactory[Float, TFloat] = floatTEF
        def extractNativeJdbcValue(rs: ResultSet, i: Int): Int = rs.getInt(i)
      }

    val optionIntTEF: IntegralTypedExpressionFactory[Option[
      Int
    ], TOptionInt, Option[Float], TOptionFloat] with DeOptionizer[Int, Int, TInt, Option[Int], TOptionInt] =
      new IntegralTypedExpressionFactory[Option[
        Int
      ], TOptionInt, Option[Float], TOptionFloat] with DeOptionizer[Int, Int, TInt, Option[Int], TOptionInt] {
        val deOptionizer: TypedExpressionFactory[Int, TInt] with JdbcMapper[Int, Int] =
          intTEF
        val floatifyer: TypedExpressionFactory[Option[Float], TOptionFloat] =
          optionFloatTEF
      }

    val longTEF: IntegralTypedExpressionFactory[Long, TLong, Double, TDouble] with PrimitiveJdbcMapper[Long] =
      new IntegralTypedExpressionFactory[Long, TLong, Double, TDouble] with PrimitiveJdbcMapper[Long] {
        val sample = 1L
        val defaultColumnLength = 8
        val floatifyer: TypedExpressionFactory[Double, TDouble] = doubleTEF
        def extractNativeJdbcValue(rs: ResultSet, i: Int): Long = rs.getLong(i)
      }

    val optionLongTEF: IntegralTypedExpressionFactory[Option[
      Long
    ], TOptionLong, Option[Double], TOptionDouble] with DeOptionizer[Long, Long, TLong, Option[Long], TOptionLong] =
      new IntegralTypedExpressionFactory[Option[
        Long
      ], TOptionLong, Option[Double], TOptionDouble] with DeOptionizer[Long, Long, TLong, Option[Long], TOptionLong] {
        val deOptionizer: TypedExpressionFactory[Long, TLong] with JdbcMapper[Long, Long] =
          longTEF
        val floatifyer: TypedExpressionFactory[Option[Double], TOptionDouble] =
          optionDoubleTEF
      }

    val bigDecimalTEF: FloatTypedExpressionFactory[BigDecimal, TBigDecimal] with PrimitiveJdbcMapper[BigDecimal] =
      new FloatTypedExpressionFactory[BigDecimal, TBigDecimal] with PrimitiveJdbcMapper[BigDecimal] {
        val sample = BigDecimal(1)
        val defaultColumnLength: Int = -1
        def extractNativeJdbcValue(rs: ResultSet, i: Int): BigDecimal = {
          val v = rs.getBigDecimal(i)
          if (rs.wasNull())
            null
          else
            BigDecimal(v)
        }
      }

    val optionBigDecimalTEF: FloatTypedExpressionFactory[Option[BigDecimal], TOptionBigDecimal]
      with DeOptionizer[BigDecimal, BigDecimal, TBigDecimal, Option[
        BigDecimal
      ], TOptionBigDecimal] =
      new FloatTypedExpressionFactory[Option[BigDecimal], TOptionBigDecimal]
        with DeOptionizer[BigDecimal, BigDecimal, TBigDecimal, Option[
          BigDecimal
        ], TOptionBigDecimal] {
        val deOptionizer: TypedExpressionFactory[BigDecimal, TBigDecimal] with JdbcMapper[BigDecimal, BigDecimal] =
          bigDecimalTEF
      }
  }

  initialize()

  protected def initialize(): Option[FieldAttributesBasedOnType[_]] = {
    import PrimitiveTypeSupport._

    register(byteTEF)
    register(intTEF)
    register(longTEF)
    register(floatTEF)
    register(doubleTEF)
    register(bigDecimalTEF)

    register(binaryTEF)
    register(booleanTEF)
    register(stringTEF)
    register(timestampTEF)
    register(dateTEF)
    register(sqlDateTEF)
    register(uuidTEF)
    register(intArrayTEF)
    register(longArrayTEF)
    register(doubleArrayTEF)
    register(stringArrayTEF)

    val re: JdbcMapper[Int, Enumeration#Value]
      with TypedExpressionFactory[Enumeration#Value, TEnumValue[
        Enumeration#Value
      ]] = enumValueTEF(DummyEnum.DummyEnumerationValue)

    /** Enumerations are treated differently, since the map method should normally return the actual Enumeration#value, but given that an enum is not only
      * determined by the int value from the DB, but also the parent Enumeration parentEnumeration.values.find(_.id == v), the conversion is done in
      * FieldMetaData.canonicalEnumerationValueFor(i: Int)
      */
    val z = new FieldAttributesBasedOnType[Any](
      new {
        def map(rs: ResultSet, i: Int): Int = rs.getInt(i)
        def convertToJdbc(v: AnyRef): AnyRef = v
      },
      re.defaultColumnLength,
      re.sample,
      classOf[java.lang.Integer]
    )

    registry.put(z.clasz, z)
    registry.put(z.clasz.getSuperclass, z)
  }

  protected type MapperForReflection = {
    def map(rs: ResultSet, i: Int): Any
    def convertToJdbc(v: AnyRef): AnyRef
  }

  protected def makeMapper(fa0: JdbcMapper[_, _]): Object {

    def map(rs: ResultSet, i: Int): AnyRef

    def convertToJdbc(v: AnyRef): AnyRef
  } = new {
    val fa: JdbcMapper[AnyRef, AnyRef] =
      fa0.asInstanceOf[JdbcMapper[AnyRef, AnyRef]]

    def map(rs: ResultSet, i: Int): AnyRef = fa.map(rs, i)

    def convertToJdbc(v: AnyRef): AnyRef = {
      if (v != null)
        fa.convertToJdbc(v)
      else null
    }
  }

  protected class FieldAttributesBasedOnType[A](
      val mapper: MapperForReflection,
      val defaultLength: Int,
      val sample: A,
      val nativeJdbcType: Class[_]
  ) {

    val clasz: Class[_] = sample.asInstanceOf[AnyRef].getClass

    override def toString: String =
      clasz.getCanonicalName + " --> " + mapper.getClass.getCanonicalName
  }

  def nativeJdbcValueFor(nonNativeType: Class[_], r: AnyRef): AnyRef =
    get(nonNativeType).mapper.convertToJdbc(r)

  def isSupported(c: Class[_]): Boolean =
    lookup(c).isDefined ||
      c.isAssignableFrom(classOf[Some[_]]) ||
      classOf[Product1[Any]].isAssignableFrom(c)

  def defaultColumnLength(c: Class[_]): Int =
    get(c).defaultLength

  def nativeJdbcTypeFor(c: Class[_]): Class[_] =
    get(c).nativeJdbcType

  def resultSetHandlerFor(c: Class[_]): (ResultSet, Int) => AnyRef = {
    val fa = get(c)
    (rs: ResultSet, i: Int) => {
      val z = fa.mapper.map(rs, i)
      if (rs.wasNull) null
      else z.asInstanceOf[AnyRef]
    }
  }

  private def get(c: Class[_]) = lookup(c).getOrElse(
    Utils.throwError(s"Unsupported native type ${c.getCanonicalName},${c.getName}\n${registry.mkString("\n")}")
  )

  def sampleValueFor(c: Class[_]): AnyRef =
    get(c).sample.asInstanceOf[AnyRef]

  def trySampleValueFor(c: Class[_]): AnyRef = {
    val r = lookup(c).map(_.sample)
    r match {
      case Some(x: AnyRef) => x
      case _               => null
    }
  }

  private[squeryl] def register[P, A](
      m: NonPrimitiveJdbcMapper[P, A, _]
  ): Unit = {

    val z = new FieldAttributesBasedOnType(
      makeMapper(m),
      m.defaultColumnLength,
      m.sample,
      m.primitiveMapper.nativeJdbcType
    )

    val wasThere = registry.put(z.clasz, z)

    if (wasThere.isDefined)
      Utils.throwError(
        "field type " + z.clasz + " already registered, handled by " + m.getClass.getCanonicalName
      )
  }

  private[squeryl] def register[S, J](m: ArrayJdbcMapper[S, J]): Unit = {
    val f = m.thisTypedExpressionFactory
    val z = new FieldAttributesBasedOnType(
      makeMapper(m),
      m.defaultColumnLength,
      f.sample,
      m.nativeJdbcType
    )

    val wasThere = registry.put(z.clasz, z)

    if (wasThere.isDefined)
      Utils.throwError(
        "field type " + z.clasz + " already registered, handled by " + m.getClass.getCanonicalName
      )
  }

  private def register[A](pm: PrimitiveJdbcMapper[A]): Unit = {
    val f = pm.thisTypedExpressionFactory
    val z = new FieldAttributesBasedOnType(
      makeMapper(pm),
      f.defaultColumnLength,
      f.sample,
      pm.nativeJdbcType
    )

    val c = z.clasz

    registry.put(c, z)
  }

  private def lookup(c: Class[_]): Option[FieldAttributesBasedOnType[_]] = {
    if (!c.isPrimitive)
      registry.get(c)
    else
      c.getName match {
        case "int"     => registry.get(classOf[java.lang.Integer])
        case "long"    => registry.get(classOf[java.lang.Long])
        case "float"   => registry.get(classOf[java.lang.Float])
        case "byte"    => registry.get(classOf[java.lang.Byte])
        case "boolean" => registry.get(classOf[java.lang.Boolean])
        case "double"  => registry.get(classOf[java.lang.Double])
        case "void"    => None
      }
  }
}
