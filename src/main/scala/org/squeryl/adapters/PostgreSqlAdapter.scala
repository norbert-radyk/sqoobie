package org.squeryl.adapters

import org.squeryl.dsl.ast.FunctionNode
import java.sql.{ResultSet, SQLException}
import java.util.UUID
import org.squeryl.internals.{StatementWriter, DatabaseAdapter, FieldMetaData}
import org.squeryl.{Session, Table}

class PostgreSqlAdapter extends DatabaseAdapter {

  /** NB: You can override `usePostgresSequenceNamingScheme` to return true in a child class to change the sequence naming behavior to align with the default
    * postgresql scheme.
    */
  def usePostgresSequenceNamingScheme: Boolean = false

  override def intTypeDeclaration = "integer"
  override def stringTypeDeclaration = "varchar"
  override def stringTypeDeclaration(length: Int): String = s"varchar($length)"
  override def booleanTypeDeclaration = "boolean"
  override def doubleTypeDeclaration = "double precision"
  override def longTypeDeclaration = "bigint"
  override def bigDecimalTypeDeclaration = "numeric"
  override def bigDecimalTypeDeclaration(precision: Int, scale: Int): String = s"numeric($precision,$scale)"
  override def binaryTypeDeclaration = "bytea"
  override def uuidTypeDeclaration = "uuid"

  override def jdbcIntArrayCreationType = "int4"
  override def jdbcLongArrayCreationType = "int8"
  override def jdbcDoubleArrayCreationType = "float8"
  override def jdbcStringArrayCreationType = "varchar"

  override def foreignKeyConstraintName(foreignKeyTable: Table[_], idWithinSchema: Int): String =
    foreignKeyTable.name + "FK" + idWithinSchema

  override def postCreateTable(t: Table[_], printSinkWhenWriteOnlyMode: Option[String => Unit]): Unit = {

    val autoIncrementedFields =
      t.posoMetaData.fieldsMetaData.filter(_.isAutoIncremented)

    for (fmd <- autoIncrementedFields) {
      val sw = new StatementWriter(false, this)
      sw.write("create sequence ", quoteName(fmd.sequenceName))

      if (printSinkWhenWriteOnlyMode.isEmpty) {
        val st = Session.currentSession.connection.createStatement
        st.execute(sw.statement)
      } else
        printSinkWhenWriteOnlyMode.get.apply(sw.statement + ";")
    }
  }

  def sequenceName(t: Table[_]): String =
    if (usePostgresSequenceNamingScheme) {
      // This is compatible with the default postgresql sequence naming scheme.
      val autoIncPK =
        t.posoMetaData.fieldsMetaData.find(fmd => fmd.isAutoIncremented)
      t.name + "_" + autoIncPK.get.nameOfProperty + "_seq"
    } else {
      // Use the legacy Squeryl sequence naming scheme.
      t.prefixedPrefixedName("seq_")
    }

  override def createSequenceName(fmd: FieldMetaData): String =
    if (usePostgresSequenceNamingScheme) {
      // This is compatible with the default postgresql sequence naming scheme.
      fmd.parentMetaData.viewOrTable.name + "_" + fmd.columnName + "_seq"
    } else {
      // Use the legacy Squeryl sequence naming scheme.
      super.createSequenceName(fmd)
    }

  override def writeConcatFunctionCall(fn: FunctionNode, sw: StatementWriter): Unit =
    sw.writeNodesWithSeparator(fn.args, " || ", newLineAfterSeparator = false)

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
    val colVals = List(
      "nextval('" + quoteName(autoIncPK.get.sequenceName) + "')"
    ) ::: f.map(fmd => writeValue(o_, fmd, sw)).toList

    sw.write("insert into ")
    sw.write(quoteName(t.prefixedName))
    sw.write(" (")
    sw.write(colNames.map(fmd => quoteName(fmd.columnName)).mkString(", "))
    sw.write(") values ")
    sw.write(colVals.mkString("(", ",", ")"))
  }

  /** In the case custom DB type used it is beneficial to explicitly cast value to its type, because it invokes proper cast function. For example, it is
    * possible to insert Scala String into a DB ENUM using dbType.
    */
  override protected def writeValue(
      o: AnyRef,
      fmd: FieldMetaData,
      sw: StatementWriter
  ): String =
    fmd.explicitDbTypeDeclaration match {
      case Some(declaration) if fmd.explicitDbTypeCast =>
        val original = super.writeValue(o, fmd, sw)
        original + "::" + declaration
      case _ => super.writeValue(o, fmd, sw)
    }

  override def supportsAutoIncrementInColumnDeclaration: Boolean = false

  override def isTableDoesNotExistException(e: SQLException): Boolean =
    e.getSQLState.equals("42P01")

  override def writeCompositePrimaryKeyConstraint(t: Table[_], cols: Iterable[FieldMetaData]): String = {
    val tableName = quoteName(t.prefixedName)
    val columns = cols.map(column => quoteName(column.columnName)).mkString(",")
    s"alter table $tableName add primary key ($columns)"
  }

  override def writeDropForeignKeyStatement(foreignKeyTable: Table[_], fkName: String): String = {
    val tableName = quoteName(foreignKeyTable.prefixedName)
    val constraintName = quoteName(fkName)
    s"alter table $tableName drop constraint $constraintName"
  }

  override def failureOfStatementRequiresRollback = true

  override def postDropTable(t: Table[_]): Unit = {

    val autoIncrementedFields =
      t.posoMetaData.fieldsMetaData.filter(_.isAutoIncremented)

    for (fmd <- autoIncrementedFields) {
      val sw = new StatementWriter(this)
      sw.write("drop sequence " + quoteName(fmd.sequenceName))
      execFailSafeExecute(sw, e => e.getSQLState.equals("42P01"))
    }
  }

  override def quoteIdentifier(s: String): String =
    List("\"", s.replace("\"", "\"\""), "\"").mkString

  override def convertFromUuidForJdbc(u: UUID): AnyRef = u
  override def convertToUuidForJdbc(rs: ResultSet, i: Int): UUID =
    rs.getObject(i).asInstanceOf[UUID]
}
