package com.socrata.config

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.socrata.api.VersionResponse
import io.quarkus.runtime.annotations.StaticInitSafe
import io.smallrye.config.ConfigMapping

@StaticInitSafe
@ConfigMapping(prefix = "version")
//For now we will cheat and say our version response and config are the same
trait VersionConfig extends VersionResponse
