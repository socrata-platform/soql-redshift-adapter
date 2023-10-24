import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{SecondaryWatcherApp, SecondaryWatcherAppConfig}
import com.socrata.thirdparty.metrics.MetricsReporter
import config.RedshiftSecondaryConfig
import config.RedshiftSecondaryDependencies.SecondaryMap
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import org.apache.curator.framework.CuratorFramework

@QuarkusMain
class Application
(
  secondaryBundle: (DSInfo, MetricsReporter, CuratorFramework),
  secondaryWatcherAppConfig: RedshiftSecondaryConfig,
  secondaryMap: SecondaryMap
) extends QuarkusApplication {
  override def run(args: String*): Int = {
    val (dsInfo: DSInfo, reporter: MetricsReporter, curator: CuratorFramework) = secondaryBundle
    SecondaryWatcherApp(dsInfo, reporter, curator)(secondaryWatcherAppConfig)(secondaryMap)
    0
  }
}
