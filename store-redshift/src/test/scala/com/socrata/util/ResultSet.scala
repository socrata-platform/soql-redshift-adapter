package com.socrata.util

object ResultSet {

  def extractHeadOption[T](resultSet: java.sql.ResultSet)(mappingFunction: java.sql.ResultSet => T): Option[T] = {
    extractThen(resultSet)(mappingFunction)(_.headOption)
  }

  def extractThen[T, M](resultSet: java.sql.ResultSet)(mappingFunction: java.sql.ResultSet => T)(collectingFunction: BufferedIterator[T] => M): M = {
    collectingFunction(extract(resultSet)(mappingFunction))
  }


  def extract[T](resultSet: java.sql.ResultSet)(mappingFunction: java.sql.ResultSet => T): BufferedIterator[T] =
    toIterator(resultSet)(mappingFunction).buffered

  def toIterator[T](resultSet: java.sql.ResultSet)(mappingFunction: java.sql.ResultSet => T): Iterator[T] = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = mappingFunction(resultSet)
    }
  }

  def toList[T](resultSet: java.sql.ResultSet)(mappingFunction: java.sql.ResultSet => T): List[T] = {
    toIterator(resultSet)(mappingFunction).toList
  }

}
