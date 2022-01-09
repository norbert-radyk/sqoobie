package org.squeryl.internals

import org.squeryl.dsl.{PrimitiveJdbcMapper, TypedExpressionFactory}

trait BasicTEF[X, TX] extends TypedExpressionFactory[X, TX] with PrimitiveJdbcMapper[X]
