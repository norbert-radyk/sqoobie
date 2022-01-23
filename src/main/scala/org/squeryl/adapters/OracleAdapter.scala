package org.squeryl.adapters

import org.squeryl.dsl.ast._
import org.squeryl.internals.{ConstantStatementParam, DatabaseAdapter, FieldMetaData, StatementWriter}
import org.squeryl.{InternalFieldMapper, Session, Table}

import java.sql.SQLException
import scala.collection.immutable.List
import scala.collection.{Set, mutable}

class OracleAdapter extends DatabaseAdapter {

  override def intTypeDeclaration = "number"
  override def stringTypeDeclaration = "varchar2"
  override def stringTypeDeclaration(length: Int): String = "varchar2(" + length + ")"
  override def booleanTypeDeclaration = "number(1)"
  override def doubleTypeDeclaration = "double precision"
  override def longTypeDeclaration = "number"
  override def binaryTypeDeclaration = "blob"
  override def timestampTypeDeclaration = "timestamp"

  override def supportsAutoIncrementInColumnDeclaration: Boolean = false

  override def supportsUnionQueryOptions = false

  override def postCreateTable(
      t: Table[_],
      printSinkWhenWriteOnlyMode: Option[String => Unit]
  ): Unit = {

    val autoIncrementedFields =
      t.posoMetaData.fieldsMetaData.filter(_.isAutoIncremented)

    for (fmd <- autoIncrementedFields) {

      val sw = new StatementWriter(false, this)
      sw.write(
        "create sequence ",
        fmd.sequenceName,
        " start with 1 increment by 1 nomaxvalue"
      )

      if (printSinkWhenWriteOnlyMode.isEmpty) {
        val st = Session.currentSession.connection.createStatement
        st.execute(sw.statement)
      } else
        printSinkWhenWriteOnlyMode.get.apply(sw.statement + ";")
    }
  }

  override def postDropTable(t: Table[_]): Unit = {

    val autoIncrementedFields =
      t.posoMetaData.fieldsMetaData.filter(_.isAutoIncremented)

    for (fmd <- autoIncrementedFields) {
      val sw = new StatementWriter(this)
      sw.write("drop sequence " + fmd.sequenceName)
      execFailSafeExecute(sw, e => e.getErrorCode == 2289)
    }
  }

  override def createSequenceName(fmd: FieldMetaData): String = {
    val prefix = "s_" + fmd.columnName.take(6) + "_" + fmd.parentMetaData.viewOrTable.name.take(10)

    // prefix is no longer than 19, we will pad it with a suffix no longer than 11 :
    val shrunkName = prefix +
      generateAlmostUniqueSuffixWithHash(
        fmd.columnName + "_" + fmd.parentMetaData.viewOrTable.name
      )

    shrunkName
  }

  override def writeInsert[T](o: T, t: Table[T], sw: StatementWriter): Unit = {

    val o_ = o.asInstanceOf[AnyRef]

    val autoIncPK =
      t.posoMetaData.fieldsMetaData.find(fmd => fmd.isAutoIncremented)

    if (autoIncPK.isEmpty) {
      super.writeInsert(o, t, sw)
      return
    }

    val f = getInsertableFields(t.posoMetaData.fieldsMetaData)

    val colNames = List(autoIncPK.get) ::: f.toList
    val colVals = List(autoIncPK.get.sequenceName + ".nextval") ::: f
      .map(fmd => writeValue(o_, fmd, sw))
      .toList

    sw.write("insert into ")
    sw.write(t.prefixedName)
    sw.write(" (")
    sw.write(colNames.map(fmd => fmd.columnName).mkString(", "))
    sw.write(") values ")
    sw.write(colVals.mkString("(", ",", ")"))
  }

  override def writeConcatFunctionCall(fn: FunctionNode, sw: StatementWriter): Unit =
    sw.writeNodesWithSeparator(fn.args, " || ", newLineAfterSeparator = false)

  override def writeJoin(
      queryableExpressionNode: QueryableExpressionNode,
      sw: StatementWriter
  ): Unit = {
    sw.write(queryableExpressionNode.joinKind.get._1)
    sw.write(" ")
    sw.write(queryableExpressionNode.joinKind.get._2)
    sw.write(" join ")
    queryableExpressionNode.write(sw)
    sw.write(" ")
    sw.write(queryableExpressionNode.alias)
    sw.write(" on ")
    queryableExpressionNode.joinExpression.get.write(sw)
  }

  override def writePaginatedQueryDeclaration(
      page: () => Option[(Int, Int)],
      qen: QueryExpressionElements,
      sw: StatementWriter
  ): Unit = {}

  override def writeQuery(qen: QueryExpressionElements, sw: StatementWriter): Unit =
    if (qen.page.isEmpty)
      super.writeQuery(qen, sw)
    else {
      sw.write("select sq____1.* from (")
      sw.nextLine
      sw.writeIndented {
        sw.write("select sq____0.*, rownum as rn____")
        sw.nextLine
        sw.write("from")
        sw.nextLine
        sw.writeIndented {
          sw.write("(")
          super.writeQuery(qen, sw)
          sw.write(") sq____0")
        }
      }
      sw.nextLine
      sw.write(") sq____1")
      sw.nextLine
      sw.write("where")
      sw.nextLine
      sw.writeIndented {
        sw.write("rn____ between ")
        val page = qen.page.get
        val beginOffset = page._1 + 1
        val endOffset = page._2 + beginOffset - 1
        sw.write(beginOffset.toString)
        sw.write(" and ")
        sw.write(endOffset.toString)
      }
    }

  override def isTableDoesNotExistException(e: SQLException): Boolean =
    e.getErrorCode == 942

  def legalOracleSuffixChars: List[Char] =
    OracleAdapter.legalOracleSuffixChars

  def paddingPossibilities(start: String, padLength: Int): Iterable[String] =
    if (padLength < 0)
      org.squeryl.internals.Utils
        .throwError("padLength must be positive, was given : " + padLength)
    else if (padLength == 0)
      Seq(start)
    else if (padLength == 1)
      legalOracleSuffixChars.map(start + _)
    else
      for (
        end <- legalOracleSuffixChars;
        pad <- paddingPossibilities(start, padLength - 1)
      )
        yield pad + end

  class CouldNotShrinkIdentifierException extends RuntimeException

  def makeUniqueInScope(
      s: String,
      scope: Set[String],
      padLength: Int
  ): String = {

    val prefix = s.substring(0, s.length - padLength)
    val possibilities = paddingPossibilities(prefix, padLength)

    for (p <- possibilities if !scope.contains(p))
      return p

    if (s.length == padLength) // at this point 's' is completely 'random like', not helpful to add it in the error message
      throw new CouldNotShrinkIdentifierException

    makeUniqueInScope(s, scope, padLength + 1)
  }

  def makeUniqueInScope(
      s: String,
      scope: scala.collection.Set[String]
  ): String =
    try {
      if (scope.contains(s))
        makeUniqueInScope(s, scope, 1)
      else
        s
    } catch {
      case _: CouldNotShrinkIdentifierException =>
        org.squeryl.internals.Utils.throwError("could not make a unique identifier with '" + s + "'")
    }

  def shrinkTo30AndPreserveUniquenessInScope(
      identifier: String,
      scope: mutable.HashSet[String]
  ): String =
    if (identifier.length <= 29)
      identifier
    else {
      val res = makeUniqueInScope(identifier.substring(0, 30), scope)
      scope.add(res)
      res
    }

  override def writeSelectElementAlias(se: SelectElement, sw: StatementWriter): Unit =
    sw.write(shrinkTo30AndPreserveUniquenessInScope(se.aliasSegment, sw.scope))

  override def foreignKeyConstraintName(
      foreignKeyTable: Table[_],
      idWithinSchema: Int
  ): String = {
    val name = super.foreignKeyConstraintName(foreignKeyTable, idWithinSchema)
    val r = shrinkTo30AndPreserveUniquenessInScope(
      name,
      foreignKeyTable.schema._namingScope
    )
    r
  }

  override def writeRegexExpression(
      left: ExpressionNode,
      pattern: String,
      sw: StatementWriter
  ): Unit = {
    sw.write(" REGEXP_LIKE(")
    left.write(sw)
    sw.write(",?)")
    sw.addParam(
      ConstantStatementParam(
        InternalFieldMapper.stringTEF.createConstant(pattern)
      )
    )
  }

  override def fieldAlias(n: QueryableExpressionNode, fse: FieldSelectElement): String =
    "f" + fse.uniqueId.get

  override def aliasExport(
      parentOfTarget: QueryableExpressionNode,
      target: SelectElement
  ): String =
    // parentOfTarget.alias + "_" + target.aliasSegment
    "f" + target.actualSelectElement.id

  override def viewAlias(vn: ViewExpressionNode[_]): String =
    "t" + vn.uniqueId.get
  /*
  override def writeCastInvocation(e: TypedExpression[_,_], sw: StatementWriter) = {
    sw.write("cast(")
    e.write(sw)

    val dbSpecificType = databaseTypeFor(e.mapper.jdbcClass)

    sw.write(" as ")

    if(dbSpecificType == stringTypeDeclaration)
      sw.write(stringTypeDeclaration(1024))
    else
      sw.write(dbSpecificType)

    sw.write(")")
  }
   */
}

object OracleAdapter {

  val legalOracleSuffixChars: List[Char] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789".toCharArray.toList
}
