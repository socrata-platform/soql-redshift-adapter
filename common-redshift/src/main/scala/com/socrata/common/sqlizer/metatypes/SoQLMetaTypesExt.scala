package com.socrata.common.sqlizer.metatypes

import com.socrata.soql.analyzer2.MetaTypes
import com.socrata.soql.sqlizer.MetaTypesExt

import com.socrata.common.sqlizer._

trait SoQLMetaTypesExt extends MetaTypesExt { this: MetaTypes =>
  type ExtraContext = SoQLExtraContext
  type ExtraContextResult = SoQLExtraContext.Result
  type CustomSqlizeAnnotation = Nothing
  type SqlizerError = RedshiftSqlizerError[ResourceNameScope]
}
