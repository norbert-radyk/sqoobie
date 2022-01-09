package org.squeryl.internals

import org.squeryl.dsl.{DeOptionizer, TypedExpressionFactory}

trait BasicOptionTEF[X, TX, TOptionX] extends TypedExpressionFactory[Option[X], TOptionX] with DeOptionizer[X, X, TX, Option[X], TOptionX]
