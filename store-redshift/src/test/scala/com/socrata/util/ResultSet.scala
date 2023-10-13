package com.socrata.util

import java.sql.ResultSet

object ResultSet {

  def extract[T](res: ResultSet)(f: ResultSet => T): Stream[T] = {
    new Iterator[T] {
      def hasNext = res.next()

      def next() = f(res)
    }.toStream
  }

}
