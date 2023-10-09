package config;


import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;

@StaticInitSafe
@ConfigMapping(prefix = "redshift")
public interface RedshiftSecondaryConfig {

}
