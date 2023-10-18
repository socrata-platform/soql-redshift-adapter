package config

import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.Secondary
import com.socrata.datacoordinator.secondary.SecondaryWatcherApp.NumWorkers
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.metrics.MetricsReporter
import config.RedshiftSecondaryDependencies.{SecondaryMap, SecondaryBundle}
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.apache.curator.framework.CuratorFramework

object RedshiftSecondaryDependencies{
  type SecondaryBundle = (DSInfo, MetricsReporter, CuratorFramework)
  type SecondaryWithWorkers = (Secondary[SoQLType, SoQLValue], NumWorkers)
  type SecondaryMap = Map[String, SecondaryWithWorkers]
}

@ApplicationScoped
class RedshiftSecondaryDependencies {

  @Produces
  def dsInfo():DSInfo = ???

  @Produces
  def metricsReporter(): MetricsReporter = ???

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
