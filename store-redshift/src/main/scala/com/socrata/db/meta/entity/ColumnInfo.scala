package com.socrata.db.meta.entity

import jakarta.persistence.{Column, Entity, ManyToOne, Table, UniqueConstraint}


@Entity
@Table(name = "column_map", uniqueConstraints = Array(
  new UniqueConstraint(columnNames = Array("copy_system_id", "user_column_id")),
  new UniqueConstraint(columnNames = Array("copy_system_id", "is_system_primary_key")),
  new UniqueConstraint(columnNames = Array("copy_system_id", "is_user_primary_key")),
  new UniqueConstraint(columnNames = Array("copy_system_id", "is_version")),
  new UniqueConstraint(columnNames = Array("copy_system_id", "field_name_casefolded"))
))
class ColumnInfo extends SocrataEntityBase {
  @ManyToOne
  @Column(name = "copy_system_id")
  var copyInfo: CopyInfo = _
  @Column(name = "user_column_id")
  var userColumnId: String = _
  @Column(name = "type_name")
  var typeName: String = _
  @Column(name = "physical_column_base_base")
  var physicalColumnBaseBase: String = _
  @Column(name = "is_system_primary_key")
  var isSystemPrimaryKey: Option[Boolean] = _
  @Column(name = "is_user_primary_key")
  var isUserPrimaryKey: Option[Boolean] = _
  @Column(name = "is_version")
  var isVersion: Option[Boolean] = _
  @Column(name = "field_name")
  var fieldName: Option[String] = _
  @Column(name = "field_name_casefolded")
  var fieldNameCaseFolded: Option[String] = _
}
