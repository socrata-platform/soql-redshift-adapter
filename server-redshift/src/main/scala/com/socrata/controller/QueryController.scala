package com.socrata.controller

import com.socrata.api.QueryApi
import com.socrata.model.{QueryRequest, QueryResponse}
import jakarta.enterprise.context.ApplicationScoped

import java.{lang, util}

@ApplicationScoped
class QueryController extends QueryApi {
  override def queryDatasetGet(dataset: String, query: String, rowCount: Integer, copy: String, rollupName: String, obfuscatedId: lang.Boolean, queryTimeoutSeconds: Integer, xSocrataDebug: String, xSocrataAnalyze: String, context: String): QueryResponse = ???

  override def queryDatasetPost(dataset: String, queryRequest: QueryRequest, rowCount: Integer, copy: String, rollupName: String, obfuscatedId: lang.Boolean, queryTimeoutSeconds: Integer, xSocrataDebug: String, xSocrataAnalyze: String, context: String): util.List[QueryResponse] = ???
}
