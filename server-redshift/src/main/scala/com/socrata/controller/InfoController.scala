package com.socrata.controller

import com.socrata.api.InfoApi
import com.socrata.model.{InfoRequest, InfoResponse}
import jakarta.enterprise.context.ApplicationScoped

import java.{lang, util}

@ApplicationScoped
class InfoController extends InfoApi {
  override def infoDatasetPost(dataset: String, infoRequest: InfoRequest, rowCount: Integer, copy: String, rollupName: String, obfuscatedId: lang.Boolean, queryTimeoutSeconds: Integer, xSocrataDebug: String, xSocrataAnalyze: String, context: String): util.List[InfoResponse] = ???
}
