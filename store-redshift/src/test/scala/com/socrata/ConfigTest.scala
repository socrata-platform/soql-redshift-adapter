package com.socrata

import com.socrata.config.{ConfigProvider, RedshiftSecondaryConfig}
import com.socrata.datacoordinator.secondary.messaging.eurybates.{EurybatesConfig, MessageProducerConfig, ZookeeperConfig}
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.logging.Log
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{DisplayName, Test}

import java.util.Properties
import scala.concurrent.duration.Duration
import scala.util.Using

@DisplayName("Config Tests")
@QuarkusTest
class ConfigTest() {
  @Inject
  var configProvider: ConfigProvider = _

  @DisplayName("one")
  @Test
  def one():Unit = {
    val redshiftSecondaryConfig: RedshiftSecondaryConfig = configProvider.proxy("redshift", classOf[RedshiftSecondaryConfig])
    assert(Duration("5 minutes").equals(redshiftSecondaryConfig.backoffInterval))
    assert(Duration("30 minutes").equals(redshiftSecondaryConfig.claimTimeout))
    assert("collocation-lock".equals(redshiftSecondaryConfig.collocationLockPath))
    assert(Duration("10s").equals(redshiftSecondaryConfig.collocationLockTimeout))
    assert("${TRUTH_CLUSTER}".equals(redshiftSecondaryConfig.instance))
    //log4j todo
    assert(Duration("2 hours").equals(redshiftSecondaryConfig.maxReplayWait))
    assert(redshiftSecondaryConfig.maxReplays.contains(200))
    assert(29.equals(redshiftSecondaryConfig.maxRetries))

    val messageProducerConfig:MessageProducerConfig = redshiftSecondaryConfig.messageProducerConfig.get

    val eurybates:EurybatesConfig = messageProducerConfig.eurybates
    assert("activemq".equals(eurybates.producers))
    assert("tcp://local.dev.socrata.net:61616".equals(eurybates.activemqConnStr))

    val zookeeper:ZookeeperConfig = messageProducerConfig.zookeeper
    assert("local.dev.socrata.net:2181".equals(zookeeper.connSpec))
    assert(zookeeper.sessionTimeout.equals(4))

    assert(Duration("2 minutes").equals(redshiftSecondaryConfig.replayWait))


  }

}
