package com.socrata.controller

import com.socrata.api.VersionApi
import com.socrata.model.VersionInfo
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class VersionController extends VersionApi {
  override def versionGet(): VersionInfo = ???
}
