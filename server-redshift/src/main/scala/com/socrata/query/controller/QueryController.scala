package com.socrata.query.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.query.api.{QueryEndpoint, QueryEndpointPostParams}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.util.{SoQLAnalyzerHelper, StreamUtil}
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.core.Response

import java.io.InputStream

@ApplicationScoped class QueryController(objectMapper: ObjectMapper) extends QueryEndpoint {
  override def post(params: QueryEndpointPostParams, query: InputStream): Response = {
    val analysis: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = SoQLAnalyzerHelper.deserialize(StreamUtil.normalize(query))
    Response.ok(Map("params" -> objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(params), "analysis" -> analysis)).build()
  }
}
