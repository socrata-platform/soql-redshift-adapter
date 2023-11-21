package com.socrata.config

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import com.socrata.config.CommonObjectMapperCustomizer
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.Secondary
import com.socrata.datacoordinator.secondary.SecondaryWatcherApp.NumWorkers
import com.socrata.datacoordinator.secondary.messaging.eurybates.MessageProducerConfig
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.metrics.{Metrics, MetricsOptions, MetricsReporter}
import com.socrata.config.RedshiftSecondaryDependencies.{SecondaryBundle, SecondaryMap}
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.json.JsonObject
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.eclipse.microprofile.config.ConfigProvider
import com.socrata.service.RedshiftSecondary
import io.quarkus.arc.All

import scala.collection.JavaConverters._
import java.io.{File, OutputStream}
import java.sql.Connection
import java.util.{Properties, UUID}
import javax.sql.{DataSource => JavaDataSource}
import scala.concurrent.duration.FiniteDuration

object RedshiftSecondaryDependencies {
  type SecondaryBundle = (DSInfo, MetricsReporter, CuratorFramework)
  type SecondaryWithWorkers = (Secondary[SoQLType, SoQLValue], NumWorkers)
  type SecondaryMap = Map[String, SecondaryWithWorkers]
}

@ApplicationScoped
class RedshiftSecondaryDependencies {

  @Produces
  def objectMapperCustomizer(): ObjectMapperCustomizer = new CommonObjectMapperCustomizer


  @Produces
  def configSource():ConfigSource = JacksonYamlConfigSource("application.yaml")

  @Produces
  def configProvider
  (
    objectMapper:ObjectMapper,
    @All
    configSources: java.util.List[ConfigSource]
  ): ConfigProvider={
    JacksonProxyConfigBuilder(objectMapper)
      .withSources(configSources.asScala.toArray:_*)
  }

  @Produces
  def redshiftSecondaryConfig
  (
    configProvider:ConfigProvider
  ): RedshiftSecondaryConfig = {
    configProvider.proxy("redshift",classOf[RedshiftSecondaryConfig])
  }

  @Produces
  def dsInfo
  (
    @DataSource("store")
    storeDataSource: AgroalDataSource
  ): DSInfo = {
    new DSInfo {
      override val dataSource: JavaDataSource = storeDataSource
      //TODO ??
      override val copyIn = RedshiftCopyIn
    }
  }

  @Produces
  def metricsOptions(): MetricsOptions = {
    MetricsOptions()
  }

  @Produces
  def metricsRegistry(): MetricRegistry = {
    Metrics.metricsRegistry
  }

  @Produces
  def metricsReporter
  (
    metricsOptions: MetricsOptions,
    metricRegistry: MetricRegistry
  ): MetricsReporter = {
    new MetricsReporter(metricsOptions, metricRegistry)
  }

  @Produces
  def curator(zkConfig: ZookeeperConfig): CuratorFramework = {
    val curator = CuratorFrameworkFactory.newClient(
      zkConfig.ensemble,
      new BoundedExponentialBackoffRetry(
        zkConfig.baseSleepTimeMs,
        zkConfig.maxSleepTimeMs,
        zkConfig.maxRetries))
    curator.start()
    curator
  }

  @Produces
  def coreBundle
  (
    dsInfo: DSInfo,
    metricsReporter: MetricsReporter,
    curatorFramework: CuratorFramework
  ): SecondaryBundle = (dsInfo, metricsReporter, curatorFramework)

  @Produces
  def secondaries(secondary: RedshiftSecondary): SecondaryMap = Map("redshift" -> (secondary, 1))

  //TODO: Map keys will need to be dynamic

  object RedshiftCopyIn extends ((Connection, String, OutputStream => Unit) => Long) {
    def apply(conn: Connection, sql: String, output: OutputStream => Unit): Long =
      ???
  }


}
