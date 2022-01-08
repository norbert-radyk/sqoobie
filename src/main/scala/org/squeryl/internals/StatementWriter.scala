
package org.squeryl.internals

import org.squeryl.dsl.ast.{ExpressionNode}
import collection.mutable.{HashSet, ArrayBuffer}
import org.squeryl.dsl.ast.ConstantTypedExpression
import org.squeryl.dsl.ast.ConstantExpressionNodeList


trait StatementParam

case class ConstantStatementParam(p: ConstantTypedExpression[_,_]) extends StatementParam
case class FieldStatementParam(v: AnyRef, fmd: FieldMetaData) extends StatementParam
/*
 * ParamWithMapper is a workadound to accommodate the ConstantExpressionNodeList, ideally 'in' and 'notIn' would grab the TEF in scope :
 * 
 * def in[A2,T2](t: Traversable[A2])(implicit cc: CanCompare[T1,T2], tef: TypedExpressionFactory[A2,T2]): LogicalBoolean =  
 *   new InclusionOperator(this, new RightHandSideOfIn(new zConstantExpressionNodeList(t, mapper)).toIn)
 * 
 * type inferencer doesn't like it, so I grab the mapper that is available, which is JDBC compatible, so in practive it should work 
 * all the time...
 * */
case class ConstantExpressionNodeListParam(v: AnyRef, l: ConstantExpressionNodeList[_]) extends StatementParam

/**
 * @param isForDisplay: when true, users of StatementWriter should write
 *   jdbc params as strings in statement,
 *   otherwise a jdbc param declarations '?' should be written, and
 *   the param values should be accumulated with addParam(s)
 */
class StatementWriter(val isForDisplay: Boolean, val databaseAdapter: DatabaseAdapter) {
  outer =>

  def this(databaseAdapter: DatabaseAdapter) = this(false, databaseAdapter)

  val scope = new HashSet[String]

  protected val _paramList = new ArrayBuffer[StatementParam]

  /**
   * a surrogate writer will accumulate text within itself (not the parent)
   * while param accumulation (addParam) will go to the root writer, this
   * is useful when it is easier to first build a string and to write it
   * afterwards
   */
  def surrogate:StatementWriter = new StatementWriter(isForDisplay, databaseAdapter) {
    
    indentWidth = outer.indentWidth
    
    override def surrogate = outer.surrogate

    override def addParam(p: StatementParam) = outer.addParam(p)
  }

  def params: Iterable[StatementParam] = _paramList

  private[this] val _stringBuilder = new java.lang.StringBuilder(256)

  def statement = _stringBuilder.toString

  def addParam(p: StatementParam) = _paramList.append(p)

  override def toString =
    if(_paramList.isEmpty)
      statement
    else
      _paramList.mkString(statement+"\njdbcParams:[",",","]")
  
  private[this] val INDENT_INCREMENT = 2
  
  private[this] var indentWidth = 0

  def indent(width: Int) = indentWidth += width
  def unindent(width: Int) = indentWidth -= width

  def indent: Unit = indent(INDENT_INCREMENT)
  def unindent: Unit = unindent(INDENT_INCREMENT)

  private def _append(s: String) = {
    _flushPendingNextLine
    _stringBuilder.append(s)
  }

  private def _writeIndentSpaces: Unit = 
    _writeIndentSpaces(indentWidth)
  
  private def _writeIndentSpaces(c: Int) =
    for( i <- 1 to c)
      _append(" ")

  def nextLine = {
    _append("\n")
    _writeIndentSpaces
  }

  private[this] var _lazyPendingLine: Option[() => Unit] = None

  def pushPendingNextLine =
   _lazyPendingLine = Some(()=> nextLine)

  private def _flushPendingNextLine =
    if(_lazyPendingLine != None)  {
      val pl = _lazyPendingLine
      _lazyPendingLine = None
      val lpl = pl.get
      lpl()
   }
  
  def writeLines(s: String*) = {
    val size = s.size
    val c = 1

    for(l <- s) {
      _append(l)
      if(c < size)
        nextLine
    }
  }

  def writeLinesWithSeparator(s: Iterable[String], separator: String) = {
    val size = s.size
    var c = 1
    for(l <- s) {
      _append(l)
      if(c < size)
        _append(separator)
      nextLine
      c += 1
    }
  }

  def writeNodesWithSeparator(s: Iterable[ExpressionNode], separator: String, newLineAfterSeparator: Boolean) = {
    val size = s.size
    var c = 1
    for(n <- s) {
      n.write(this)
      if(c < size) {
        _append(separator)
        if(newLineAfterSeparator)
          nextLine
      }
      c += 1
    }
  }

  def write(s: String*) =
    for(s0 <- s)
      _append(s0)

  def writeIndented(u: =>Unit): Unit =
    writeIndented(INDENT_INCREMENT, u)

  def writeIndented(width: Int, u: =>Unit) = {
    indent(width)
    _writeIndentSpaces(width)
    u
    unindent(width)
  }

  def quoteName(s: String) = databaseAdapter.quoteName(s)
}
