package com.socrata.config;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;

@StaticInitSafe
@ConfigMapping(prefix = "zookeeper")
public interface ZookeeperConfig {
    String ensemble();

    Integer baseSleepTimeMs();

    Integer maxSleepTimeMs();

    Integer maxRetries();
}
