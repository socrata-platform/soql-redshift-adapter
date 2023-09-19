package com.socrata.controller

import com.socrata.api.{RollupsApi, RollupsRequest}
import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.truth.metadata.UnanchoredRollupInfo
import io.smallrye.mutiny.Multi
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class RollupsController extends RollupsApi {
  override def getRollups(rollupsRequest: RollupsRequest): Multi[UnanchoredRollupInfo] = {
    Multi.createFrom().items(
      UnanchoredRollupInfo(new RollupName("rollup_one"), "select *")
    )
  }
}
