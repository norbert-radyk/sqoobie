
package org.squeryl.dsl.ast

import org.squeryl.internals.{FieldMetaData, ResultSetMapper, StatementWriter}
import org.squeryl.{Session, View}

import scala.collection.mutable

class ViewExpressionNode[U](val view: View[U])
  extends QueryableExpressionNode {

  private[this] val _selectElements = new mutable.HashMap[FieldMetaData,SelectElement]

  def isChild(q: QueryableExpressionNode) = false

  def getOrCreateAllSelectElements(forScope: QueryExpressionElements): Iterable[SelectElement] = {

    val shouldExport = !forScope.isChild(this)
    view.posoMetaData.fieldsMetaData.map(fmd =>
      getOrCreateSelectElement(fmd, shouldExport)
    )
  }

  private def getOrCreateSelectElement(fmd: FieldMetaData, shouldExport: Boolean): SelectElement = {

    val e = _selectElements.get(fmd)
    val n =
      if(e.isDefined)
        e.get
      else {
        val r = new FieldSelectElement(this, fmd, resultSetMapper)
        _selectElements.put(fmd, r)
        r
      }

    if(shouldExport)
      new ExportedSelectElement(n)
    else
      n
  }

  def getOrCreateSelectElement(fmd: FieldMetaData): SelectElement =
    getOrCreateSelectElement(fmd, shouldExport = false)

  def getOrCreateSelectElement(fmd: FieldMetaData, forScope: QueryExpressionElements): SelectElement =
    getOrCreateSelectElement(fmd, !forScope.isChild(this))


  val resultSetMapper = new ResultSetMapper

  def alias =
    Session.currentSession.databaseAdapter.viewAlias(this)

  def owns(aSample: AnyRef) = aSample eq sample.asInstanceOf[AnyRef]

  private[this] var _sample: Option[U] = None

  private[squeryl] def sample_=(d:U) =
    _sample = Some(d)

  def sample = _sample.get

  def doWrite(sw: StatementWriter) =
      sw.write(sw.quoteName(view.prefixedName))

  override def toString = {
    val sb = new java.lang.StringBuilder
    sb.append("'ViewExpressionNode[")
    sb.append(sample)
    sb.append("]:")
    sb.append("rsm=")
    sb.append(resultSetMapper)
    sb.toString
  }
}
