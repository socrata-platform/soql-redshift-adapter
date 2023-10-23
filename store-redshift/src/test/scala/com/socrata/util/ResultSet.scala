package com.socrata.util

import java.sql.ResultSet

object ResultSet {

  def extract[T](resultSet: ResultSet)(mappingFunction: ResultSet => T): BufferedIterator[T] = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = mappingFunction(resultSet)
    }.buffered
  }

  def extractThen[T,M](resultSet: ResultSet)(mappingFunction: ResultSet => T)(collectingFunction: BufferedIterator[T] => M): M={
    collectingFunction(extract(resultSet)(mappingFunction))
  }

  def extractHeadOption[T](resultSet: ResultSet)(mappingFunction: ResultSet => T): Option[T] = {
    extractThen(resultSet)(mappingFunction)(_.headOption)
  }

}
