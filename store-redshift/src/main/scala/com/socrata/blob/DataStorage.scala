package com.socrata.store.blob

import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import scala.collection.mutable.ArrayBuilder
import java.io._
import scala.collection.JavaConverters._

trait DataStorage {
  def store(key: String, data: Iterator[Array[Byte]]): Option[Long]
}

case class DataStorageImpl(s3Client: AmazonS3)(bucketName: String, partsSize: Int) extends DataStorage {
  private def cleanupFailedUpload(uploadId: String, key: String): Unit = {
    s3Client.abortMultipartUpload(
      new AbortMultipartUploadRequest(bucketName, key, uploadId)
    )
  }

  private def withCleanup[A](uploadId: String, key: String)(a: => A) =
    try {
      a
    } catch {
      case e: Throwable =>
        cleanupFailedUpload(uploadId, key)
        throw e
    }

  private def groupBySize(i: Iterator[Array[Byte]])(chunkSize: Int) =
    new Iterator[Array[Byte]] {
      private def fill() = {
        val buf = ArrayBuilder.make[Byte]
        var ptr = 0
        while (i.hasNext && ptr < chunkSize) {
          val toCopy = i.next()
          ptr += toCopy.size
          buf ++= toCopy
        }
        buf.result
      }

      def hasNext: Boolean = i.hasNext

      def next(): Array[Byte] = {
        if (i.hasNext) {
          return fill
        } else {
          throw new NoSuchElementException("next on empty iterator")
        }
      }
    }

  private def partRequest(
      key: String,
      uploadId: String,
      part: Array[Byte],
      size: Int,
      partNum: Int,
      last: Boolean = false
  ) = {
    new UploadPartRequest()
      .withUploadId(uploadId)
      .withBucketName(bucketName)
      .withKey(key)
      .withPartNumber(partNum)
      .withPartSize(size)
      .withInputStream(new ByteArrayInputStream(part))
      .withLastPart(last)
  }

  private def uploadPart(req: UploadPartRequest): PartETag =
    s3Client.uploadPart(req).getPartETag()

  private def requests(key: String, uploadId: String)(it: BufferedIterator[Array[Byte]]) =
    it.zipWithIndex
      .map { case (data, num) => (data, num + 1) }
      .map { case (data, num) => partRequest(key, uploadId, data, data.size, num, !it.hasNext) -> num }
      .map { case (req, num) => uploadPart(req) -> num }
      .map { case (res, _) => res }

  private def terminateMultipartUpload(
      key: String,
      uploadId: String,
      etags: List[PartETag]
  ): Unit =
    s3Client.completeMultipartUpload(
      new CompleteMultipartUploadRequest(
        bucketName,
        key,
        uploadId,
        etags.asJava
      )
    )

  override def store(
      key: String,
      data: Iterator[Array[Byte]]
  ): Option[Long] = {
    if (data.isEmpty) None
    else {
      val uploadId = s3Client
        .initiateMultipartUpload(
          new InitiateMultipartUploadRequest(bucketName, key)
        )
        .getUploadId
      withCleanup(uploadId, key) {
        val grouped = groupBySize(data)(partsSize)
        val etags = requests(key, uploadId)(grouped.buffered).toList
        terminateMultipartUpload(key, uploadId, etags)
        Some(s3Client.getObjectMetadata(bucketName, key).getContentLength())
      }
    }
  }
}
