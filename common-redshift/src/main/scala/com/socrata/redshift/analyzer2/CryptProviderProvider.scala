package com.socrata.redshift.analyzer2

import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.obfuscation.CryptProvider

trait CryptProviderProvider {
  def forProvenance(provenenace: Provenance): Option[CryptProvider]
}
