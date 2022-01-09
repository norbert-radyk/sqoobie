package org.squeryl.adapters

import org.squeryl.Schema
import org.squeryl.internals.{StatementWriter, FieldMetaData, DatabaseAdapter}
import org.squeryl.dsl.ast._
import java.sql.SQLException

class DerbyAdapter extends DatabaseAdapter {

  override def intTypeDeclaration = "integer"
  override def binaryTypeDeclaration = "blob(1M)"

  override def isFullOuterJoinSupported = false

  override def writeColumnDeclaration(
      fmd: FieldMetaData,
      isPrimaryKey: Boolean,
      schema: Schema
  ): String = {

    var res =
      "  " + quoteIdentifier(fmd.columnName) + " " + databaseTypeFor(fmd)

    for (d <- fmd.defaultValue) {
      val v = convertToJdbcValue(d.value.asInstanceOf[AnyRef])
      if (v.isInstanceOf[String])
        res += " default '" + v + "'"
      else
        res += " default " + v
    }

    if (!fmd.isOption)
      res += " not null"

    if (isPrimaryKey)
      res += " primary key"

    if (supportsAutoIncrementInColumnDeclaration && fmd.isAutoIncremented)
      res += " generated always as identity"

    res
  }

  override def writePaginatedQueryDeclaration(
      page: () => Option[(Int, Int)],
      qen: QueryExpressionElements,
      sw: StatementWriter
  ) = {
    page().foreach(p => {
      sw.write("offset ")
      sw.write(p._1.toString)
      sw.write(" rows fetch next ")
      sw.write(p._2.toString)
      sw.write("rows only")
      sw.pushPendingNextLine
    })
  }

  override def isTableDoesNotExistException(e: SQLException) =
    e.getSQLState == "42Y55"

  override def writeRegexExpression(
      left: ExpressionNode,
      pattern: String,
      sw: StatementWriter
  ) =
    throw new UnsupportedOperationException(
      "Derby does not support a regex scalar function"
    )

  override def quoteIdentifier(s: String) = "\"" + s + "\""
}
