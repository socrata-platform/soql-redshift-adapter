package com.socrata

import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{SecondaryWatcherApp}
import com.socrata.thirdparty.metrics.MetricsReporter
import config.RedshiftSecondaryConfig
import config.RedshiftSecondaryDependencies.SecondaryMap
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import com.amazonaws.services.s3.{AmazonS3ClientBuilder}

@QuarkusMain
class Application extends QuarkusApplication {
  override def run(args: String*): Int = {
    println("================================================================", 88)
    0
  }
}
