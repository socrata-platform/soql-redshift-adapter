package com.socrata.config

import com.socrata.config.RedshiftSecondaryDependencies.SecondaryBundle
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.thirdparty.metrics.MetricsReporter
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry

@ApplicationScoped
class ZookeeperDependencies {

  @Produces
  def curator(zkConfig: ZookeeperConfig): CuratorFramework = {
    val curator = CuratorFrameworkFactory.newClient(
      zkConfig.ensemble,
      new BoundedExponentialBackoffRetry(
        zkConfig.baseSleepTimeMs,
        zkConfig.maxSleepTimeMs,
        zkConfig.maxRetries
      )
    )
    curator.start()
    curator
  }

  @Produces
  def coreBundle(
                  dsInfo: DSInfo,
                  metricsReporter: MetricsReporter,
                  curatorFramework: CuratorFramework
                ): SecondaryBundle = (dsInfo, metricsReporter, curatorFramework)
}
