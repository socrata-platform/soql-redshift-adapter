package com.socrata.model

import com.socrata.soql.environment.ColumnName

case class QualifiedColumnName(qualifier: Option[String], columnName: ColumnName)
