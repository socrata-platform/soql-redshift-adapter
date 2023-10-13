package com.socrata.service

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.socrata.util.Timing
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import jakarta.enterprise.context.ApplicationScoped

import java.io.File
import java.sql.ResultSet
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

  def extract[T](res: ResultSet)(f: ResultSet => T): Stream[T] = {
    new Iterator[T] {
      def hasNext = res.next()

      def next() = f(res)
    }.toStream
  }

  def getTableRowCount(tableName:String):Option[Long]={
    Using.resource(dataSource.getConnection) { conn =>
      Using.resource(conn.prepareStatement(s"""select count(*) from "$tableName";""")) { stmt =>
        val resultset = stmt.executeQuery();
        val rowcount:Option[Long] = extract(resultset)(rs => rs.getLong(1)).headOption;
        rowcount match{
          case Some(count)=> println(s"Table $tableName has $count records")
          case _=> println(s"Unable to get record count for table $tableName")
        }
        rowcount
      }
    }
  }

  def insertJdbc[T](tableName: String, columnNames: Array[String], batchSize: Long, iterator: Iterator[Array[T]]): Unit = {
    assert(batchSize >= 1)
    println(s"Batch size: $batchSize")
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
            println(s"Executing batch #$currentBatch, total: $count")
            Timing.Timed {
              stmt.executeLargeBatch()
              stmt.clearParameters()
              stmt.clearBatch()
            } { elapsed =>
              println(s"Batch #$currentBatch took $elapsed")
            }
          } else {
            currentBatch = (count / batchSize) + 1
          }
        }
        if(count % batchSize!=0){
          println(s"Executing final batch #$currentBatch, total: $count")
          Timing.Timed {
            stmt.executeLargeBatch()
            stmt.clearParameters()
            stmt.clearBatch()
          } { elapsed =>
            println(s"Batch #$currentBatch took $elapsed")
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
      s3.deleteObject(bucketName, fileName);
    }

  }

}
