package config;


import com.socrata.datacoordinator.secondary.SecondaryWatcherAppConfig;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;

@StaticInitSafe
@ConfigMapping(prefix = "redshift")
public interface RedshiftSecondaryConfig extends SecondaryWatcherAppConfig { }

@StaticInitSafe
@ConfigMapping(prefix = "zookeeper")
public interface ZookeeperConfig {
    String ensemble;
    Int baseSleepTimeMs;
    Int maxSleepTimeMs;
    Int maxRetries;
}
