package com.socrata.controller

import com.socrata.api.SchemaApi
import com.socrata.model.DatasetSchema
import jakarta.enterprise.context.ApplicationScoped

import java.lang

@ApplicationScoped
class SchemaController extends SchemaApi {
  override def schemaGet(copy: String, fieldName: lang.Boolean, ds: String, rn: String): DatasetSchema = ???
}
