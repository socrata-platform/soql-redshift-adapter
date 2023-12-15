package com.socrata.db.meta.entity

import jakarta.persistence.{Column, Entity, JoinColumn, ManyToOne, Table, UniqueConstraint}

import java.time.LocalDateTime

@Entity
@Table(name = "copy_map",uniqueConstraints = Array(new UniqueConstraint(columnNames = Array("dataset_system_id", "copy_number"))))
class CopyInfo extends SocrataEntityBase {
  @ManyToOne
  @JoinColumn(name = "dataset_system_id")
  var datasetInfo: DatasetInfo = _
  @Column(name = "copy_number")
  var copyNumber: Long = _
  @Column(name = "lifecycle_stage")
  var lifecycleStage: String = _
  @Column(name = "data_version")
  var dataVersion: Long = _
  @Column(name = "last_modified")
  var lastModified: LocalDateTime = _
  @Column(name = "data_shape_version")
  var dataShapeVersion: Option[Long] = _
}

object CopyInfo{
  def apply(datasetInfo: DatasetInfo,copyNumber: Long,lifecycleStage: String,dataVersion: Long,lastModified: LocalDateTime,dataShapeVersion: Option[Long]):CopyInfo = {
    val out = new CopyInfo
    out.datasetInfo=datasetInfo
    out.copyNumber=copyNumber
    out.lifecycleStage=lifecycleStage
    out.datasetInfo=datasetInfo
    out.lastModified=lastModified
    out.dataShapeVersion=dataShapeVersion
    out
  }
}
