package com.socrata.controller

import com.socrata.api.{VersionApi, VersionResponse}
import com.socrata.config.VersionConfig
import io.smallrye.mutiny.Uni
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class VersionController(config: VersionConfig) extends VersionApi {
  override def getVersion(): Uni[VersionResponse] = {
    //Simply return our config for now
    Uni.createFrom().item(
      config
    )
  }
}
