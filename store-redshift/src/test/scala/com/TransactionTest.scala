package com

import com.socrata.common.db.meta.entity.Dataset
import com.socrata.common.db.meta.repository.DatasetRepository
import com.socrata.datacoordinator.id.CopyId
import com.socrata.datacoordinator.secondary.{
  CopyInfo,
  DatasetInfo,
  LifecycleStage
}
import io.quarkus.logging.Log
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import jakarta.transaction.Transactional
import org.joda.time.DateTime
import org.junit.jupiter.api.Test

import scala.util.{Failure, Success, Try}

@QuarkusTest
class TransactionTest {
  @Inject
  var datasetRepository: DatasetRepository = _

  @Test
  def successfulPersist(): Unit = {
    val internalName: String = "alpha50"
    val copyNumber: Long = 20L
    val datasetInfo: DatasetInfo =
      DatasetInfo(internalName, "en", Array.empty, Some("aaaa-aaaa"))
    val copyInfo: CopyInfo = CopyInfo(
      new CopyId(10),
      copyNumber,
      LifecycleStage.Published,
      1L,
      1L,
      DateTime.now()
    )
    val dataset: Dataset = Dataset(datasetInfo, copyInfo)
    persist(dataset)

    findViaInteralNameAndCopyNumber(internalName, copyNumber) match {
      case Some(persistedDataset) => {
        Log.info("dataset persisted, now deleting")
        delete(persistedDataset)
      }
      case None =>
        assume(false, "expected dataset to be persisted successfully")
    }
  }
  @Test
  def persistInterruptedByException(): Unit = {
    val internalName: String = "alpha50"
    val copyNumber: Long = 20L
    val datasetInfo: DatasetInfo =
      DatasetInfo(internalName, "en", Array.empty, Some("aaaa-aaaa"))
    val copyInfo: CopyInfo = CopyInfo(
      new CopyId(10),
      copyNumber,
      LifecycleStage.Published,
      1L,
      1L,
      DateTime.now()
    )
    val dataset: Dataset = Dataset(datasetInfo, copyInfo)
    Try(persistWithException(dataset)) match {
      case Failure(_) => {
        assert(
          !findViaInteralNameAndCopyNumber(internalName, copyNumber).isDefined,
          "dataset should not have been persisted"
        )
      }
      case Success(_) =>
        assume(false, "this was supposed to fail due to an exception")
    }

  }

  @Transactional
  def persist(dataset: Dataset): Unit = {
    datasetRepository.persist(dataset)
  }

  @Transactional
  def findViaInteralNameAndCopyNumber(
      internalName: String,
      copyNumber: Long
  ): Option[Dataset] = {
    datasetRepository.findByInternalNameAndCopyNumber(internalName, copyNumber)
  }

  @Transactional
  def delete(dataset: Dataset): Unit = {
    datasetRepository.delete(dataset)
  }

  @Transactional
  def persistWithException(dataset: Dataset): Unit = {
    datasetRepository.persist(dataset)
    throw new IllegalArgumentException("noooo")
  }

}
