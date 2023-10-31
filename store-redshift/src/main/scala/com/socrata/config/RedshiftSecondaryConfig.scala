package com.socrata.config

import com.socrata.datacoordinator.secondary.SecondaryWatcherAppConfig
import com.socrata.datacoordinator.secondary.messaging.eurybates.MessageProducerConfig

import java.io.File
import java.util.{Properties, UUID}
import scala.concurrent.duration.FiniteDuration

class RedshiftSecondaryConfig
(
 val backoffInterval: FiniteDuration ,
 val claimTimeout: FiniteDuration ,
 val collocationLockPath: String ,
 val collocationLockTimeout: FiniteDuration ,
 val instance: String ,
 val log4j: Properties ,
 val maxReplayWait: FiniteDuration ,
 val maxReplays: Option[Int] ,
 val maxRetries: Int ,
 val messageProducerConfig: Option[MessageProducerConfig] ,
 val replayWait: FiniteDuration ,
 val tmpdir: File ,
 val watcherId: UUID
)  extends SecondaryWatcherAppConfig{

}
