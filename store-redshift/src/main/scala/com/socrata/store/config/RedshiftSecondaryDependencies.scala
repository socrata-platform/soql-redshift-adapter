package com.socrata.store.config

import RedshiftSecondaryDependencies.SecondaryMap
import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import com.socrata.common.config.{CommonObjectMapperCustomizer, ConfigProvider, ConfigSource, EnvSource, JacksonProxyConfigBuilder, JacksonYamlConfigSource, PropertiesFileEnvSource}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.Secondary
import com.socrata.datacoordinator.secondary.SecondaryWatcherApp.NumWorkers
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.store.service.RedshiftSecondary
import com.socrata.thirdparty.metrics.{Metrics, MetricsOptions, MetricsReporter}
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.arc.All
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.apache.curator.framework.CuratorFramework

import java.io.OutputStream
import java.sql.Connection
import javax.sql.{DataSource => JavaDataSource}
import scala.collection.JavaConverters._

object RedshiftSecondaryDependencies {
  type SecondaryBundle = (DSInfo, MetricsReporter, CuratorFramework)
  type SecondaryWithWorkers = (Secondary[SoQLType, SoQLValue], NumWorkers)
  type SecondaryMap = Map[String, SecondaryWithWorkers]
}

@ApplicationScoped
class RedshiftSecondaryDependencies {

  @Produces
  def objectMapperCustomizer() = new CommonObjectMapperCustomizer

  @Produces
  def configSource(): ConfigSource = JacksonYamlConfigSource("application.yaml")

  @Produces
  def envSource(): EnvSource = PropertiesFileEnvSource(".env")

  @Produces
  def configProvider(
      objectMapper: ObjectMapper,
      @All
      configSources: java.util.List[ConfigSource],
      @All
      envSources: java.util.List[EnvSource]
  ): ConfigProvider = JacksonProxyConfigBuilder(objectMapper)
    .withSources(configSources.asScala.toArray: _*)
    .withEnvs(envSources.asScala.toArray: _*)

  @Produces
  def redshiftSecondaryConfig(
      configProvider: ConfigProvider
  ): RedshiftSecondaryConfig = configProvider.proxy("redshift", classOf[RedshiftSecondaryConfig])

  @Produces
  def dsInfo(
      @DataSource("store")
      storeDataSource: AgroalDataSource
  ): DSInfo = new DSInfo {
    override val dataSource: JavaDataSource = storeDataSource
    // TODO ??
    override val copyIn = RedshiftCopyIn
  }

  @Produces
  def metricsOptions(): MetricsOptions = MetricsOptions()

  @Produces
  def metricsRegistry(): MetricRegistry = Metrics.metricsRegistry

  @Produces
  def metricsReporter(
      metricsOptions: MetricsOptions,
      metricRegistry: MetricRegistry
  ) = new MetricsReporter(metricsOptions, metricRegistry)

  @Produces
  def secondaries(secondary: RedshiftSecondary): SecondaryMap = Map("redshift" -> (secondary, 1))

  // TODO: Map keys will need to be dynamic

  object RedshiftCopyIn extends ((Connection, String, OutputStream => Unit) => Long) {
    def apply(conn: Connection, sql: String, output: OutputStream => Unit): Long =
      ???
  }

}
