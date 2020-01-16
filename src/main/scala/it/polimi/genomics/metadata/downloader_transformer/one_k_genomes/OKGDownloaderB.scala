package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import it.polimi.genomics.metadata.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer.Downloader
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.FTPHelper
import org.apache.commons.net.ftp.FTPFile
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Created by Tom on ott, 2019
 *
 * This class implements a backup strategy to download and check updates for 1kGenomes datasets.
 * It explores the FTP server in the predefined locations (specified in the XML configuration file) and
 * looks for updated versions of the dataset's files by comparing the local file-size and last-modified-date with
 * respect the remote one.
 *
 * This downloader is used as backup option because
 * 1. If a file changes, its size doesn't necessarily do the same.
 * 2. FTP file timestamps are not designed to be used as markers for detecting changes in the file. Technically it's
 * possible for a server administrator re-upload a file leaving the same timestamp of the previous version.
 * Instead, hashing is the most reliable mechanism for detecting changes and that's indeed the strategy used by OKGDownloaderA.
 * However, since computing the hash of very large files such as the 1kGenomes mutations could take hours, OKGDownloaderA
 * relies on precomputed digests available on the server. If those precomputed digests would become unavailable,
 * this downloader serves as backup strategy.
 *
 * This class is involved in the download of files and update strategy only. For what concerns the identification
 * and selection of the interesting files, see the class DatasetInfo.
 */
class OKGDownloaderB extends Downloader {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * downloads the files from the source defined in the loader
   * into the folder defined in the loader
   *
   * For each dataset, download method should put the downloaded files inside
   * /source.outputFolder/dataset.outputFolder/Downloads
   *
   * @param source contains specific download and sorting info.
   */
  override def download(source: Source, parallelExecution: Boolean): Unit = {
    if(!source.downloadEnabled )
      return
    val sourceId = FileDatabase.sourceId(source.name)
    source.datasets.filter(dataset => dataset.downloadEnabled).foreach(dataset => {
      logger.info("BEGIN DOWNLOAD FOR SOURCE:DATASET "+source.name+":"+dataset.name)
      val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
      // mark with the temporary status FILE_STATUS.COMPARE any file in the dataset
      FileDatabase.markToCompare(datasetId, Stage.DOWNLOAD)
      // download and compare local vs source versions of this same dataset
      try {
        fetchUpdatesForVariants(dataset)
        fetchUpdatesForMeta(dataset)
        /* call markAsOutdated on the dataset to mark with the status OUTDATED any file which was once in the local
        copy but it's not any more present in the source. Those files have currently the status FILE_STATUS.COMPARE */
        FileDatabase.markAsOutdated(datasetId, Stage.DOWNLOAD)
      } catch {
        case ex: Exception =>
          logger.error("ERROR WHILE FETCHING UPDATES FOR FILES OF DATASET " + dataset.name + ". DETAILS: ", ex)
      }
    })
    logger.info(s"Download for ${source.name} Finished.")

    downloadFailedFiles(source, parallelExecution)
  }

  def fetchUpdatesForVariants(dataset: Dataset): Unit = {
    val variantFiles = DatasetInfo.latestVariantsFTPFile(dataset)  // of type List[(String, List[FTPFile])]
    if (variantFiles.isEmpty) {
      throw new IllegalStateException("DOWNLOAD CAN'T CONTINUE: FTP SERVER TRAVERSAL DIDN'T RETURNED ANY VARIANT")
    } else {
      // flatten information (sorting by size - smallest to biggest - is done just for debug purposes)
      val variantsInfo = flattenFTPFileInfo(variantFiles).sortBy(file => file._3)
/*      logger.info("VARIANTS FOUND: ")
      variantsInfo.foreach(file => logger.info(file._1, file._3, file._4))*/
      // check if update & download
      val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
      variantsInfo.foreach(variant => {
        val fileId = FileDatabase.fileId(datasetId, url = variant._1, Stage.DOWNLOAD, variant._2)
        if (FileDatabase.checkIfUpdateFile(fileId, hash = "", originSize = variant._3.toString, originLastUpdate = variant._4)) {
          // download the file at proper location
          new FTPHelper(dataset).downloadFile(url = variant._1, variant._3) match {
            case Failure(exception) =>
              FileDatabase.markAsFailed(fileId)
            case Success(_) =>
              // then update the database
              FileDatabase.markAsUpdated(fileId, variant._3.toString, "")
          }
        }
        /*
        If instead the local copy is already up-to-date, I don't have to do anything 'cos in that case
        FileDatabase.checkIfUpdateFile also sets the flag FILE_STATUS.UPDATED
        */
      })
    }
  }

  def fetchUpdatesForMeta(dataset: Dataset): Unit = {
    val metaFiles = DatasetInfo.metadataFTPFile(dataset)
    if (metaFiles.isEmpty) {
      throw new IllegalStateException("DOWNLOAD CAN'T CONTINUE: FTP SERVER TRAVERSAL DIDN'T RETURNED ANY METADATA")
    } else {
      // flatten information
      val metaInfo = flattenFTPFileInfo(metaFiles)
      logger.info("METADATA FOUND: ")
      metaInfo.foreach(file => logger.info(file._1, file._3, file._4))
      // check if update & download
      val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
      metaInfo.foreach(meta => {
        val fileId = FileDatabase.fileId(datasetId, url = meta._1, Stage.DOWNLOAD, meta._2)
        if (FileDatabase.checkIfUpdateFile(fileId, hash = "", originSize = meta._3.toString, originLastUpdate = meta._4)) {
          // download the file at proper location
          new FTPHelper(dataset).downloadFile(url = meta._1, meta._3) match {
            case Failure(exception) =>
              FileDatabase.markAsFailed(fileId)
            case Success(_) =>
              // then update the database
              FileDatabase.markAsUpdated(fileId, meta._3.toString, "")
          }
        }
        /*
        If instead the local copy is already up-to-date, I don't have to do anything 'cos in that case
        FileDatabase.checkIfUpdateFile also sets the flag FILE_STATUS.UPDATED
        */
      })
    }
  }

  /**
   * This method can be used in conjunction with DatasetInfo.latestVariantsFTPFile to flatten the resulting type
   * into a more manageable List of tuples, one tuple for each file and containing all the info required
   * to interact with the database.
   * @param nestedList a List[(String, List[FTPFile])] like the one returned from DatasetInfo.latestVariantsFTPFile or
   *                   DatasetInfo.latestVariantsFTPFile
   * @return a List of tuples, each tuple containing in order: file URL, filename, size, timestamp_string
   */
  def flattenFTPFileInfo(nestedList: List[(String, List[FTPFile])]): List[(String, String, Long, String)] = {
    nestedList.flatMap(fileSet => {
      val dirURL = fileSet._1
      val files = fileSet._2
      files.map(file => (
        s"${dirURL}${file.getName}",
        file.getName,
        file.getSize,
        file.getTimestamp.getTime.toString
      ))
    })
  }

  /**
   * downloads the failed files from the source defined in the loader
   * into the folder defined in the loader
   *
   * For each dataset, download method should put the downloaded files inside
   * /source.outputFolder/dataset.outputFolder/Downloads
   *
   * @param source            contains specific download and sorting info.
   * @param parallelExecution defines parallel or sequential execution
   */
  override def downloadFailedFiles(source: Source, parallelExecution: Boolean): Unit = {
    val sourceId = FileDatabase.sourceId(source.name)
    source.datasets.filter(dataset => dataset.downloadEnabled).foreach(dataset => {
      logger.info("BEGIN DOWNLOAD FAILED FILES FOR SOURCE:DATASET " + source.name + ":" + dataset.name)
      val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
      val failedFiles = FileDatabase.getFailedFiles(datasetId, Stage.DOWNLOAD)  // of type Seq[(fileId, name, copyNumber, url, hash)]
      failedFiles.foreach(file => {
        /* The methods fetchUpdatesFor... call FileDatabase.fileID and FileDatabase.checkIfUpdateFile passing the
        file size. So I'm sure the file size exists and it's valid */
        val fileSizeString = FileDatabase.getFileAllDetails(file._1).get.originSize
        // download the file at proper location
        new FTPHelper(dataset).downloadFile(url = file._4, fileSizeString.toLong) match {
          case Failure(exception) =>
            FileDatabase.markAsFailed(file._1)
          case Success(_) =>
            // then update the database
            FileDatabase.markAsUpdated(file._1, fileSizeString, "")
        }
      })
    })
  }
}
