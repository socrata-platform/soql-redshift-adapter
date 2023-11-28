package com.socrata.controller

import com.socrata.api.RollupsApi
import com.socrata.model.RollupInfo
import jakarta.enterprise.context.ApplicationScoped

import java.{lang, util}

@ApplicationScoped
class RollupsController extends RollupsApi {
  override def rollupsGet(
      ds: String,
      rn: String,
      copy: String,
      includeUnmaterialized: lang.Boolean): util.List[RollupInfo] = ???
}
