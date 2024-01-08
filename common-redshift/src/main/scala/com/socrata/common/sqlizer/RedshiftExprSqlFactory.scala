package com.socrata.common.sqlizer

import com.socrata.common.sqlizer.metatypes.SoQLMetaTypesExt
import com.socrata.prettyprint.prelude.DocLiteralHelper
import com.socrata.soql.analyzer2.MetaTypes
import com.socrata.soql.sqlizer.ExprSqlFactory

final class RedshiftExprSqlFactory[MT <: MetaTypes with SoQLMetaTypesExt]
    extends ExprSqlFactory[MT] {
  override def compress(expr: Option[Expr], rawSqls: Seq[Doc]): Doc =
    expr match {
      case Some(_: NullLiteral) =>
        d"null :: super"
      case _ =>
        ???
    }
}
