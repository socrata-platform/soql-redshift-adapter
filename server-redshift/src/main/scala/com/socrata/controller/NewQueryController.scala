package com.socrata.controller

import com.socrata.api.NewQueryApi
import com.socrata.model.NewQueryRequest
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class NewQueryController extends NewQueryApi{
  override def newQueryPost(newQueryRequest: NewQueryRequest): AnyRef = ???
}
