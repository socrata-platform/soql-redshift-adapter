package com.socrata.service

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import jakarta.enterprise.context.ApplicationScoped

import java.io.File
import java.util.UUID
import scala.util.Using

@ApplicationScoped
class InsertService
(
  @DataSource("store")
  dataSource: AgroalDataSource,
  s3: AmazonS3,
  awsCredentials: AWSCredentials
) {

  def insertJdbc[T](tableName: String, columnNames: Array[String], batchSize: Int, iterator: Iterator[Array[T]]): Unit = {
    assert(batchSize >= 1)
    val width = columnNames.length
    val sql = s"""insert into "$tableName"(${columnNames.mkString(",")}) values(${List.fill(width)("?").mkString(",")});"""
    Using.resource(dataSource.getConnection) { conn =>
      Using.resource(conn.prepareStatement(sql)) { stmt =>
        var count = 0
        for (elem <- iterator) {
          for ((elem, index) <- elem.zipWithIndex) {
            stmt.setObject(index + 1, elem)
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

  def insertS3(bucketName: String, tableName: String,file:File): Unit = {
    val uuid = UUID.randomUUID()
    val fileName = s"upload/$uuid"
    s3.putObject(bucketName, fileName, file);
    try{
      Using.resource(dataSource.getConnection) { conn =>
        Using.resource(conn.prepareStatement(
          s"""
             |copy "$tableName"
             |from 's3://$bucketName/$fileName'
             |access_key_id '${awsCredentials.getAWSAccessKeyId}'
             |secret_access_key '${awsCredentials.getAWSSecretKey}'
             |format as csv
             |ignoreheader 1;
             |""".stripMargin)) { stmt =>
          stmt.executeUpdate()
        }
      }
    }finally {
      s3.deleteObject(bucketName, fileName);
    }

  }

}
