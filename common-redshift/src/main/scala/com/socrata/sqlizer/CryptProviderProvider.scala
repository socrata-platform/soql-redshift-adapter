package com.socrata.common.sqlizer

import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.obfuscation.CryptProvider

trait CryptProviderProvider {
  def forProvenance(provenenace: Provenance): Option[CryptProvider]
}
