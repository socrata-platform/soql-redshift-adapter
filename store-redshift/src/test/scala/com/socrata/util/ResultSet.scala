package com.socrata.util

import java.sql.ResultSet

object ResultSet {

  def extract[T](res: ResultSet)(f: ResultSet => T): BufferedIterator[T] = {
    new Iterator[T] {
      def hasNext = res.next()

      def next() = f(res)
    }.buffered
  }

  def extractCollecting[T,M](res: ResultSet)(f: ResultSet => T)(m: BufferedIterator[T] => M): M={
    m(extract(res)(f))
  }

  def extractHeadOption[T](res: ResultSet)(f: ResultSet => T): Option[T] = {
    extractCollecting(res)(f)(_.headOption)
  }

}
