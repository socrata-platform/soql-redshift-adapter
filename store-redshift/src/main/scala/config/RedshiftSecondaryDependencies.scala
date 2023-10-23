package config

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import com.socrata.config.JacksonConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.Secondary
import com.socrata.datacoordinator.secondary.SecondaryWatcherApp.NumWorkers
import com.socrata.datacoordinator.secondary.messaging.eurybates.MessageProducerConfig
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.metrics.{Metrics, MetricsOptions, MetricsReporter}
import config.RedshiftSecondaryDependencies.{SecondaryBundle, SecondaryMap}
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.json.JsonObject
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.eclipse.microprofile.config.ConfigProvider
import service.RedshiftSecondary

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
  def objectMapperCustomizer(): ObjectMapperCustomizer = new JacksonConfig

  @Produces
  def redshiftSecondaryConfig
  (
  objectMapper:ObjectMapper
  ): RedshiftSecondaryConfig = {
    //Seems to be tricky to implement scala -> java config types via "static" config. This is because of spi generic parameter limitations, type erasure and no arg constructor requirements.
    //Lets simply read it as json. At this point we are past the "static" config bootstrap.
    val asJson = ConfigProvider.getConfig().getValue("redshift",classOf[JsonObject])
    //And use our configured objectMapper to marshall it. Our objectMapper knows how to convert scala -> java.
    objectMapper.convertValue(asJson,classOf[RedshiftSecondaryConfig])
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
