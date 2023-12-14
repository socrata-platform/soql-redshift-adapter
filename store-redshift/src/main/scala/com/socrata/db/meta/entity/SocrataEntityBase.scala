package com.socrata.db.meta.entity

import io.quarkus.hibernate.orm.panache.PanacheEntityBase
import jakarta.persistence.{Column, GeneratedValue, Id, MappedSuperclass}

@MappedSuperclass
class SocrataEntityBase extends PanacheEntityBase {
  @Id
  @GeneratedValue
  @Column(name = "system_id")
  //TODO generator stuffs?
  var systemId: Long = _
}
