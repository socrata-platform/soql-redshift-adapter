package com.socrata.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.socrata.api.{QueryApi, QueryRequest}
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.SoQLValue
import io.smallrye.mutiny.Multi
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class QueryController(objectMapper: ObjectMapper) extends QueryApi {

  override def getQuery(queryRequest: QueryRequest): Multi[Row] = doQuery(queryRequest)
  override def postQuery(queryRequest: QueryRequest): Multi[Row] = doQuery(queryRequest)

  def doQuery(queryRequest: QueryRequest): Multi[Row] = {
    println(objectMapper.writeValueAsString(queryRequest))
    Multi.createFrom().empty()
  }
}
