package org.squeryl.framework

import org.squeryl.{SessionFactory, Schema}

import org.squeryl.test.PrimitiveTypeModeForTests._
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

abstract class SchemaTester extends DbTestBase {
  self: DBConnector =>

  def schema: Schema

  def prePopulate(): Unit = {}

  override def beforeAll(): Unit = {

    super.beforeAll()

    sessionCreator().foreach { _ =>
      transaction {
        schema.drop
        schema.create
        try {
          prePopulate()
        } catch {
          case e: Exception =>
            println(e.getMessage)
            println(e.getStackTrace)
        }
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()

    sessionCreator().foreach { _ =>
      transaction {
        schema.drop
      }
    }
  }
}

abstract class DbTestBase
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers {
  self: DBConnector =>

  def isIgnored(testName: String): Boolean =
    sessionCreator().isEmpty || ignoredTests.contains(testName)

  def ignoredTests: List[String] = Nil

  override def beforeAll(): Unit = {
    val c = sessionCreator()
    if (c.isDefined) {
      SessionFactory.concreteFactory = c
    }
  }

  override protected def runTest(
      testName: String,
      args: org.scalatest.Args
  ): org.scalatest.Status = {
    if (isIgnored(testName))
      org.scalatest.SucceededStatus
    else
      super.runTest(testName, args)
  }

}
