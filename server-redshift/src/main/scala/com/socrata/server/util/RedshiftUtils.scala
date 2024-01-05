package com.socrata.server.util

import java.sql.Connection

import com.amazon.redshift.core.BaseConnection

object RedshiftSqlUtils {

  /** Escapes a string appropriately for use on the given connection. Supports redshift wrapped redshift connections
    * that can be discovered via Connection.unwrap.
    */
  def escapeString(conn: Connection): String => String = {
    conn match {
      case c: Connection if c.isWrapperFor(classOf[BaseConnection]) =>
        (c.unwrap(classOf[BaseConnection]).escapeString _)
      case _ => throw new RuntimeException("Unsupported connection class: " + conn.getClass)

    }
  }
}
