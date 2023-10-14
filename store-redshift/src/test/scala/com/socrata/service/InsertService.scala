package com.socrata.service

import io.quarkus.logging.Log
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.socrata.util.Timing
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

  def insertJdbc[T](tableName: String, columnNames: Array[String], batchSize: Long, iterator: Iterator[Array[T]]): Unit = {
    assert(batchSize >= 1)
    Log.info(s"Batch size: $batchSize")
    val width = columnNames.length
    val sql = s"""insert into "$tableName"(${columnNames.mkString(",")}) values(${List.fill(width)("?").mkString(",")});"""
    Using.resource(dataSource.getConnection) { conn =>
      conn.setAutoCommit(false)
      var currentBatch = 0L
      Using.resource(conn.prepareStatement(sql)) { stmt =>
        var count = 0L
        for (elem <- iterator) {
          for ((elem, index) <- elem.zipWithIndex) {
            stmt.setObject(index + 1, elem)
          }
          stmt.addBatch()
          count += 1
          if (count % batchSize == 0) {
            currentBatch = count / batchSize
            Timing.Timed {
              stmt.executeLargeBatch()
              stmt.clearParameters()
              stmt.clearBatch()
            } { elapsed =>
              Log.info(s"Batch #$currentBatch, total: $count, took: $elapsed")
            }
          } else {
            currentBatch = (count / batchSize) + 1
          }
        }
        if (count % batchSize != 0) {
          Timing.Timed {
            stmt.executeLargeBatch()
            stmt.clearParameters()
            stmt.clearBatch()
          } { elapsed =>
            Log.info(s"Batch #$currentBatch, total: $count, took: $elapsed")
          }
        }
      }
      conn.commit();
      conn.setAutoCommit(true)
    }
  }

  def insertS3(bucketName: String, tableName: String, file: File): Unit = {
    val uuid = UUID.randomUUID()
    val fileName = s"upload/$uuid"
    Log.info(s"Uploading file ${file.getAbsolutePath} to $bucketName/$fileName")
    s3.putObject(bucketName, fileName, file);
    try {
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
    } finally {
      Log.info(s"Deleting file $bucketName/$fileName")
      s3.deleteObject(bucketName, fileName);
    }

  }

}
