package config

import com.codahale.metrics.MetricRegistry
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.Secondary
import com.socrata.datacoordinator.secondary.SecondaryWatcherApp.NumWorkers
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.metrics.{Metrics, MetricsOptions, MetricsReporter}
import config.RedshiftSecondaryDependencies.{SecondaryBundle, SecondaryMap}
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}

import java.io.OutputStream
import java.sql.Connection
import javax.sql.{DataSource => JavaDataSource}

object RedshiftSecondaryDependencies{
  type SecondaryBundle = (DSInfo, MetricsReporter, CuratorFramework)
  type SecondaryWithWorkers = (Secondary[SoQLType, SoQLValue], NumWorkers)
  type SecondaryMap = Map[String, SecondaryWithWorkers]
}

@ApplicationScoped
class RedshiftSecondaryDependencies {

object RedshiftCopyIn extends ((Connection, String, OutputStream => Unit) => Long) {
  def apply(conn: Connection, sql: String, output: OutputStream => Unit): Long =
    ???
}

  @Produces
  def dsInfo
  (
    @DataSource("store")
    storeDataSource: AgroalDataSource
  ):DSInfo = {
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
    new MetricsReporter(metricsOptions,metricRegistry)
  }

  @Produces
  def curator(zkConfig: ZookeeperConfig): CuratorFramework = {
    CuratorFrameworkFactory.newClient(
      zkConfig.ensemble,
      new BoundedExponentialBackoffRetry(
        zkConfig.baseSleepTimeMs,
        zkConfig.maxSleepTimeMs,
        zkConfig.maxRetries)).start()
  }

  @Produces
  def coreBundle
  (
    dsInfo: DSInfo,
    metricsReporter: MetricsReporter,
    curatorFramework: CuratorFramework
  ): SecondaryBundle = (dsInfo, metricsReporter, curatorFramework)

  //TODO: Map keys will need to be dynamic

  @Produces
  def secondaries(secondary: RedshiftSecondary): SecondaryMap = Map("redshift" -> secondary)

}
