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
import org.apache.curator.framework.CuratorFramework

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

  @Produces
  def dsInfo
  (
    @DataSource("store")
    storeDataSource: AgroalDataSource
  ):DSInfo = {
    new DSInfo {
      override val dataSource: JavaDataSource = storeDataSource
      //TODO ??
      override val copyIn: (Connection, String, OutputStream => Unit) => Long = ???
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
  def curator(): CuratorFramework = ???

  @Produces
  def coreBundle
  (
    dsInfo: DSInfo,
    metricsReporter: MetricsReporter,
    curatorFramework: CuratorFramework
  ): SecondaryBundle = (dsInfo, metricsReporter, curatorFramework)

  @Produces
  def secondaries(): SecondaryMap = ???


}
