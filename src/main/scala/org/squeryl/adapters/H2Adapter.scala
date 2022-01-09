package org.squeryl.adapters

import org.squeryl.Schema
import java.sql.SQLException
import org.squeryl.internals.{FieldMetaData, DatabaseAdapter}

class H2Adapter extends DatabaseAdapter {

  override def uuidTypeDeclaration = "uuid"
  override def isFullOuterJoinSupported = false

  override def writeColumnDeclaration(
      fmd: FieldMetaData,
      isPrimaryKey: Boolean,
      schema: Schema
  ): String = {

    var res = "  " + fmd.columnName + " " + databaseTypeFor(fmd)

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
      res += " auto_increment"

    res
  }

  override def isTableDoesNotExistException(e: SQLException): Boolean =
    e.getErrorCode == 42102

  override def supportsCommonTableExpressions = false
}
