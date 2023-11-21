package com.socrata

import com.socrata.config.{ConfigProvider, RedshiftSecondaryConfig}
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.logging.Log
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{DisplayName, Test}

import scala.util.Using

@DisplayName("Config Tests")
@QuarkusTest
class ConfigTest() {
  @Inject
  var configProvider: ConfigProvider = _

  @DisplayName("one")
  @Test
  def one():Unit = {
    val redshiftSecondaryConfig = configProvider.proxy("redshift", classOf[RedshiftSecondaryConfig])
    println(redshiftSecondaryConfig.backoffInterval)
  }

}
