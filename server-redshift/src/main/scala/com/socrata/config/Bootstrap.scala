package com.socrata.config

import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces

@ApplicationScoped class Bootstrap {
  @Produces def jacksonObjectMapperCustomizer() = new CommonObjectMapperCustomizer
}
