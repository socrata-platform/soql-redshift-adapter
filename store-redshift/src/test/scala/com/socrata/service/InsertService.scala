package com.socrata.service

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import jakarta.enterprise.context.ApplicationScoped

import scala.util.Using

@ApplicationScoped
class InsertService
(
  @DataSource("store")
  dataSource: AgroalDataSource
) {

  def insert[T](tableName:String,columnNames:Array[String],batchSize: Int, iterator: Iterator[Array[T]]):Unit={
    assert(batchSize>=1)
    val width = columnNames.length
    val sql = s"""insert into "$tableName"(${columnNames.mkString(",")}) values(${List.fill(width)("?").mkString(",")});"""
    Using.resource(dataSource.getConnection) { conn =>
      Using.resource(conn.prepareStatement(sql)) { stmt =>
        var count = 0
        for (elem <- iterator) {
          for ((elem,index) <- elem.zipWithIndex) {
            stmt.setObject(index+1,elem)
          }
          stmt.addBatch()
          count += 1
          if (count % batchSize == 0) {
            stmt.executeUpdate()
          }
        }
        stmt.executeUpdate()
      }

    }

  }

}
