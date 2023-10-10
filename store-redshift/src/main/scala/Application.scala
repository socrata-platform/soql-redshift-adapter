import service.RedshiftSecondary
import com.socrata.datacoordinator.secondary.SecondaryWatcherApp
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.enterprise.context.ApplicationScoped

@QuarkusMain
class Application(secondary:RedshiftSecondary) extends QuarkusApplication {
  override def run(args: String*): Int = {
    SecondaryWatcherApp(config => secondary)
    0
  }
}
