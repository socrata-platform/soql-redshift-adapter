package com.socrata.common.sqlizer

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._
import com.socrata.common.sqlizer.metatypes._

final class RedshiftExprSqlFactory[MT <: MetaTypes with SoQLMetaTypesExt] extends ExprSqlFactory[MT] {
  override def compress(expr: Option[Expr], rawSqls: Seq[Doc]): Doc =
    expr match {
      case Some(_: NullLiteral) =>
        d"null :: super"
      case _ =>
        ???
    }
}
