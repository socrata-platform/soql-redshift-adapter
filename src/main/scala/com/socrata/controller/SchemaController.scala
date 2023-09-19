package com.socrata.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.socrata.api.{SchemaApi, SchemaRequest}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.metadata.SchemaWithFieldName
import com.socrata.datacoordinator.util.collection.UserColumnIdMap
import io.smallrye.mutiny.Uni
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class SchemaController(objectMapper: ObjectMapper) extends SchemaApi {
  override def getSchema(schemaRequest: SchemaRequest): Uni[SchemaWithFieldName] = {
    println(objectMapper.writeValueAsString(schemaRequest))
    Uni.createFrom().item(
      SchemaWithFieldName("hash", UserColumnIdMap.empty, new UserColumnId("id"), "locale")
    )
  }
}
