package com.socrata.server.config

import com.socrata.common.config.CommonObjectMapperCustomizer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces

@ApplicationScoped class Bootstrap {
  @Produces def jacksonObjectMapperCustomizer() = new CommonObjectMapperCustomizer

}
