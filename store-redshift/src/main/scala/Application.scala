import com.socrata.datacoordinator.secondary.SecondaryWatcherApp
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import service.RedshiftSecondary

@QuarkusMain
class Application(secondary: RedshiftSecondary) extends QuarkusApplication {
  override def run(args: String*): Int = {
    SecondaryWatcherApp(config => secondary)
    0
  }
}
