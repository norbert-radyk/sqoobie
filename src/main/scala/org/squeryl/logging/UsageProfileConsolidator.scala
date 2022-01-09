package org.squeryl.logging

import org.squeryl.Session
import org.squeryl.adapters.H2Adapter
import org.squeryl.logging.StatsSchemaTypeMode._

object UsageProfileConsolidator {

  def main(args: Array[String]): Unit =
    if (args.length < 2) {
      printUsage
    } else {

      val (dst, src) = args.map(new java.io.File(_)).splitAt(1)

      val notExists = src.filterNot(_.exists)
      if (notExists.length > 0)
        org.squeryl.internals.Utils
          .throwError("Files don't exist : \n" + notExists.mkString(",\n"))

      Class.forName("org.h2.Driver")

      val dstDb = new Session(
        java.sql.DriverManager
          .getConnection("jdbc:h2:" + dst.head.getAbsolutePath, "sa", ""),
        new H2Adapter
      )

      using(dstDb) {
        for (src_i <- src) {

          val srcDb_i = new Session(
            java.sql.DriverManager
              .getConnection("jdbc:h2:" + src_i.getAbsolutePath, "sa", ""),
            new H2Adapter
          )

          val (invocations, statements) =
            using(srcDb_i) {
              (
                StatsSchema.statementInvocations.allRows,
                StatsSchema.statements.allRows
              )
            }

          val stmtsToInsert = statements.filter(stmt =>
            StatsSchema.statements.lookup(stmt.id).isEmpty
          )
          StatsSchema.statements.insert(stmtsToInsert)

          StatsSchema.statementInvocations.insert(invocations)

        }
      }
    }

  def printUsage = {
    println("Usage : ")
    println(
      "java org.squeryl.logging.UsageProfileConsolidator <h2FileForConsolidatedStatsProfile> <list of h2 files to consolidate>"
    )
  }
}
