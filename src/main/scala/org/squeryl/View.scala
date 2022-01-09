package org.squeryl

import dsl.ast.ViewExpressionNode
import dsl.{TypedExpression, QueryDsl}
import internals._
import java.sql.ResultSet

/** This class can be used for read only tables or (database) views for an
  * updatable view, or table use Table[T]
  */
class View[T] private[squeryl] (
    _name: String,
    private[squeryl] val classOfT: Class[T],
    schema: Schema,
    _prefix: Option[String],
    val ked: Option[KeyedEntityDef[T, _]]
) extends Queryable[T] {

  private[squeryl] lazy val _callbacks =
    schema._callbacks.getOrElse(this, NoOpPosoLifecycleEventListener)

  def name = schema.tableNameFromClassName(_name)

  def prefix: Option[String] =
    if (_prefix.isDefined)
      _prefix
    else
      schema.name

  def prefixedName =
    if (prefix.isDefined)
      prefix.get + "." + name
    else
      name

  /** Suppose you have : prefix.MyTable myTable.prefixedPrefixedName("z") will
    * yield : prefix.zMyTable used for creating names for objects derived from a
    * table, ex.: a sequence
    */
  def prefixedPrefixedName(s: String) =
    if (prefix.isDefined)
      prefix.get + "." + s + name
    else
      s + name

  private[squeryl] def findFieldMetaDataForProperty(name: String) =
    posoMetaData.findFieldMetaDataForProperty(name)

  val posoMetaData = new PosoMetaData(classOfT, schema, this)

  private[squeryl] def allFieldsMetaData: Iterable[FieldMetaData] =
    posoMetaData.fieldsMetaData

  protected val _setPersisted =
    if (classOf[PersistenceStatus].isAssignableFrom(classOfT))
      (t: T) => t.asInstanceOf[PersistenceStatus]._isPersisted = true
    else
      (t: T) => {}

  private[this] val _posoFactory =
    FieldMetaData.factory.createPosoFactory(posoMetaData)

  private[squeryl] def _createInstanceOfRowObject =
    _posoFactory()

  private[squeryl] def give(
      resultSetMapper: ResultSetMapper,
      resultSet: ResultSet
  ): T = {

    var o = _callbacks.create

    if (o == null)
      o = _createInstanceOfRowObject

    resultSetMapper.map(o, resultSet)
    val t = o.asInstanceOf[T]
    _setPersisted(t)
    _callbacks.afterSelect(t.asInstanceOf[AnyRef]).asInstanceOf[T]
  }

  def lookup[K](k: K)(implicit
      ked: KeyedEntityDef[T, K],
      dsl: QueryDsl,
      toCanLookup: K => CanLookup
  ): Option[T] = {
    // TODO: find out why scalac won't let dsl be passed to another method
    import dsl._

    val q = from(this)(a =>
      dsl
        .where {
          FieldReferenceLinker
            .createEqualityExpressionWithLastAccessedFieldReferenceAndConstant(
              ked.getId(a),
              k,
              toCanLookup(k)
            )
        }
        .select(a)
    )

    val it = q.iterator

    if (it.hasNext) {
      val ret = Some(it.next())
      // Forces statement to be closed.
      it.hasNext
      ret
    } else
      None
  }

  /** Will throw an exception if the given key (k) returns no row.
    */
  def get[K](k: K)(implicit
      ked: KeyedEntityDef[T, K],
      dsl: QueryDsl,
      toCanLookup: K => CanLookup
  ): T =
    lookup(k).getOrElse(
      throw new NoSuchElementException(
        "Found no row with key '" + k + "' in " + name + "."
      )
    )

  def allRows(implicit dsl: QueryDsl): Iterable[T] = {
    import dsl._
    dsl.queryToIterable(from(this)(a => select(a)))
  }

  def viewExpressionNode: ViewExpressionNode[T] =
    new ViewExpressionNode[T](this)
}

sealed trait CanLookup

private[squeryl] case object CompositeKeyLookup extends CanLookup

private[squeryl] case object UnknownCanLookup extends CanLookup

private[squeryl] case class SimpleKeyLookup[T](
    convert: T => TypedExpression[T, _]
) extends CanLookup
