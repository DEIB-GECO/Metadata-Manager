package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.nio.file.{Files, Paths}

import it.polimi.genomics.metadata.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer.Downloader
import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.DatasetInfo.DatasetPattern
import it.polimi.genomics.metadata.step.xml
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{FTPHelper, FileUtil, PatternMatch}
import org.apache.commons.cli.MissingArgumentException
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Created by Tom on set, 2019
 *
 * IMPORTANT NOTE ON TERMINOLOGY: In this context, "tree" file is the short from for indicating a text file hosted on
 * 1kGenomes FTP server called "current.tree". This file is a sort of register containing name, type, size, last updated
 * timestamp and hash of all files hosted on the server, one for each line.
 *
 * In this class, the tree is used to quickly scan the FTP server folders and sub-folders without having to actually
 * navigate through all the directories of the server.
 *
 * This class implements the preferred strategy to download and check updates for 1kGenomes datasets:
 * 1. Download the tree file which describes the structure of the FTP server.
 * 2. Check the download tree against the local copy.
 * 3. If there're changes, determine of they refer to the files of interest for the dataset. If yes, scan the tree to
 * understand if there're updated versions of the files already own, eventually download them and update the local copy
 * 3.1. Update the local copy of the tree to the latest version and quit.
 *
 * The whole procedure fails and all dataset's files are marked as FILE_STATUS.FAILED if the download of the tree fails,
 * or if the parsing of the tree doesn't return any info. If the parsing succeeds, the download of the single files may
 * still fail due to network issues, and in such case the interested files will be marked with FILE_STATUS.FAILED.
 */
class DLStrategyA extends Downloader {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // necessary params initialized during download and shared between functions
  private var treeURL: String = _
  private var treeLocalPath: Option[String] = None


  /**
   * downloads the files from the source defined in the loader
   * into the folder defined in the loader
   *
   * For each dataset, download method should put the downloaded files inside
   * /source.outputFolder/dataset.outputFolder/Downloads
   *
   * @param source contains specific download and sorting info.
   */
  override def download(source: xml.Source, parallelExecution: Boolean): Unit = {
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
        fetchUpdatesForTreeFile(dataset)
        /* call markAsOutdated on the dataset to mark with the status OUTDATED any file which was once in the local
        copy but it's not any more present in the source. Those files have currently the status FILE_STATUS.COMPARE */
        FileDatabase.markAsOutdated(datasetId, Stage.DOWNLOAD)
      } catch {
        case ex: Exception =>
          logger.error("ERROR WHILE FETCHING UPDATES FOR FILES OF DATASET " + dataset.name + ". DETAILS: ", ex)
          /* mark as failed at least the tree file even if it's updated to trigger the update of the whole dataset on
        the next run */
          markAllFilesAsFailed(datasetId)
      }
    })
    logger.info(s"Download for ${source.name} Finished.")

    downloadFailedFiles(source, parallelExecution)
  }

  /**
   * Downloads the tree file and checks if it changed. In that case, it proceeds checking for updates of the
   * dataset variants and notify the update to the database.
   *
   * @param dataset the dataset whose files will be updated if any update is available
   */
  private def fetchUpdatesForTreeFile(dataset: Dataset): Unit = {
    // get required params from XML config file
    treeURL = dataset.getParameter("tree_file_url").getOrElse(throw new MissingArgumentException(
      "MANDATORY PARAMETER tree_file_url NOT FOUND IN CONFIGURATION XML AT DATASET LEVEL"))
    //    1
    val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
    downloadOrCopyTreeFile(dataset)
    //      2
    val computedHash = FileUtil.md5Hash(treeLocalPath.get).get
    //      3
    val fileId = FileDatabase.fileId(datasetId, treeURL, Stage.DOWNLOAD, DatasetInfo.parseFilenameFromURL(treeURL))
    // Hash is enough for comparing different versions of the file, but size is anyway required to comply with database's implementation
    if (!FileDatabase.checkIfUpdateFile(fileId, computedHash, FileUtil.size(treeLocalPath.get).toString, ""))
      markAllFilesAsUpdated(datasetId)
    else {
      // update the dataset files...
      fetchUpdatesForVariants(treeLocalPath.get, dataset)
      fetchUpdatesForMeta(treeLocalPath.get, dataset)
      //    3.1
      // then update the tree file
      /* Actually I already downloaded the tree file overwriting the old local copy if it was present, so
      I just need to notify it to the database. */
      FileDatabase.markAsUpdated(fileId, FileUtil.size(treeLocalPath.get).toString, computedHash)
    }
  }

  /**
   * Parses the tree file, searches for updated versions of the given dataset's variants, download the updates
   * and updates the database.
   *
   * @param treeLocalPath path to the local tree file
   * @param dataset the dataset whom variants will be updated
   * @throws IllegalStateException if the parsing of the tree file returns an empty set of variants.
   */
  private def fetchUpdatesForVariants(treeLocalPath: String, dataset: Dataset): Unit = {
    //        3
    val variantRecords_asLines = DatasetInfo.latestVariantsRecords(treeLocalPath, dataset)
    val variantRecords = parseVariantRecords(variantRecords_asLines).sortBy(record => record._2)  // sorting by size - from smallest to biggest - is done for debug purposes
    //TODO print list of variant records sorted by size to check sorting... Maybe sorting on strings works differently
    // adding .toLong in sortBy could do the trick
    if (variantRecords.isEmpty) {
      throw new IllegalStateException("DOWNLOAD CAN'T CONTINUE: UNABLE TO EXTRACT VARIANT SET FROM TREE LOCAL FILE")
    } else {
      val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
      val urlPrefix = DatasetInfo.getURLPrefixForRecords(dataset)
//      val limited = List(variantRecords.minBy(elem => elem._2)) // takes smallest   // debug only
      variantRecords.foreach(record => {
        val filename = DatasetInfo.parseFilenameFromURL(fileURL = record._1)
        val fileId = FileDatabase.fileId(datasetId, url = s"$urlPrefix${record._1}", Stage.DOWNLOAD, filename)
        if (FileDatabase.checkIfUpdateFile(fileId, hash = record._4, originSize = record._2, originLastUpdate = record._3)) {
          // download the file at proper location
          new FTPHelper(dataset).downloadFile(url = s"$urlPrefix${record._1}", record._2.toLong) match {
            case Failure(exception) =>
              FileDatabase.markAsFailed(fileId)
            case Success(_) =>
              // then update the database
              FileDatabase.markAsUpdated(fileId, size = record._2, hash = record._4)
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
   * Parses the tree file, searches for updated versions of the given dataset's metadata, download the updates
   * and updates the database.
   *
   * @param treeLocalPath path to the local tree file
   * @param dataset the dataset whom metadata will be updated
   * @throws IllegalStateException if the parsing of the tree file returns an empty set of metadata files.
   */
  def fetchUpdatesForMeta(treeLocalPath: String, dataset: Dataset): Unit = {
    //    3
    val metaRecords_asLines = DatasetInfo.metadataRecords(treeLocalPath, dataset)
    val metaRecords = parseMetadataRecords(metaRecords_asLines)
    if (metaRecords.isEmpty) {
      throw new IllegalStateException("DOWNLOAD CAN'T CONTINUE: UNABLE TO EXTRACT METADATA INFO FROM TREE LOCAL FILE")
    } else {
      val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
      val urlPrefix = DatasetInfo.getURLPrefixForRecords(dataset)
      metaRecords.foreach(record => {
        val filename = DatasetInfo.parseFilenameFromURL(fileURL = record._1)
        val fileId = FileDatabase.fileId(datasetId, url = s"$urlPrefix${record._1}", Stage.DOWNLOAD, filename)
        if (FileDatabase.checkIfUpdateFile(fileId, hash = record._4, originSize = record._2, originLastUpdate = record._3)) {
          // download the file at proper location
          new FTPHelper(dataset).downloadFile(url = s"$urlPrefix${record._1}", record._2.toLong) match {
            case Failure(exception) =>
              FileDatabase.markAsFailed(fileId)
            case Success(_) =>
              // then update the database
              FileDatabase.markAsUpdated(fileId, size = record._2, hash = record._4)
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
   * This method extracts the needed information to compare them with the local dataset and to eventually download them.
   *
   * @param variantRecords a list of variant records belonging to the tree file
   * @return a list of tuples, one for each record, containing in order: file path, size and timestamp as Strings.
   */
  def parseVariantRecords(variantRecords: List[String]): List[(String, String, String, String)] = {
    if(variantRecords.isEmpty)
      throw new IllegalArgumentException("NO VARIANTS FOUND. PLEASE REVIEW THE DATASET PARAMETER dataset_remote_base_directory OF YOUR XML CONFIG FILE")
    val variantsWithHashes = variantRecords.map(record => {
      val parts = PatternMatch.matchParts(record, (new DatasetPattern).get())   // the pattern used affects the position and kind of info obtained
      // here is possible to return any of the field that populate the tree. I return file path, size, timestamp string and the hash.
      if(parts.size < 4)
        logger.error("THE PARSING OF RECORD: "+record+" DIDN'T PRODUCE THE EXPECTED NUMBER OF INFO. THE DOWNLOAD" +
          "OF THE VARIANT FILE MAY FAIL.")
      (parts.head, parts(2), parts(3), parts.last)
    })
    variantsWithHashes
  }

  /**
   * Obtains the records corresponding to the metadata files specified in the XML config file. Then it extracts the needed
   * information to compare them with the local dataset and to eventually download them.
   *
   * @param metadataRecords a list of metadata records belonging to the tree file
   * @return a list of tuples, one for each metadata record, containing in order: file path, size and timestamp as Strings.
   */
  def parseMetadataRecords(metadataRecords: List[String]): List[(String, String, String, String)] = {
    val metaWithDigests = metadataRecords.map(record => {
      val parts = PatternMatch.matchParts(record, (new DatasetPattern).get()) // the pattern used affects the position and kind of info obtained
      // here is possible to return any of the field that populate the tree. I return file path, size, timestamp string and the hash.
      if(parts.size < 4)
        logger.error("THE PARSING OF RECORD: "+record+" DIDN'T PRODUCE THE EXPECTED NUMBER OF INFO. THE DOWNLOAD" +
          "OF THE METADATA FILE MAY FAIL.")
      (parts.head, parts(2), parts(3), parts.last)
    })
    metaWithDigests
  }

  /**
   * The tree file is a single one for the entire FTP server. If already downloaded within this
   * run, it's copied to the other datasets instead of being downloaded again.
   * @param dataset the dataset requiring the tree file.
   * @throws java.lang.Exception if this download fails.
   */
  private def downloadOrCopyTreeFile(dataset: Dataset): Unit ={
    // download if never downloaded
    if(treeLocalPath.isEmpty) {
      new FTPHelper(dataset).downloadFile(treeURL) match {
        case Failure(exception) =>
          throw new Exception("DOWNLOAD OF TREE FILE FAILED", exception)
        case Success(value) => treeLocalPath = Some(value)
      }
    } else {
      // copy to dataset's location otherwise
      val newPath = s"${DatasetInfo.getDownloadDir(dataset)}${FileUtil.getFileNameFromPath(treeLocalPath.get)}"
      if(!Files.exists(Paths.get(newPath)))
        FileUtil.copyFile(treeLocalPath.get, newPath)
      treeLocalPath = Some(newPath)
    }
  }

  def markAllFilesAsUpdated(datasetId: Int): Unit ={
    for (file <- FileDatabase.getFilesToProcess(datasetId, Stage.DOWNLOAD)) {
      // file contains fileID, name, copy number
      val fileDetails = FileDatabase.getFileAllDetails(file._1).get // get hash, size, last update
      FileDatabase.markAsUpdated(file._1, fileDetails.size)
    }
  }

  def markAllFilesAsFailed(datasetId: Int): Unit = {
    for (file <- FileDatabase.getFilesToProcess(datasetId, Stage.DOWNLOAD)) {
      // file contains fileID, name, copy number
      FileDatabase.markAsFailed(file._1)
    }
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
      logger.info("BEGIN DOWNLOAD FAILED FILES FOR SOURCE:DATASET "+source.name+":"+dataset.name)
      val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
      val failedFiles = FileDatabase.getFailedFiles(datasetId, Stage.DOWNLOAD)  // of type Seq[(fileId, name, copyNumber, url, hash)]
      /* BEWARE getFailedFiles returns fileIDs of files with status FAILED as well as those with both status UPDATED and size="" !!! */
      if(failedFiles.nonEmpty) {
        treeURL = dataset.getParameter("tree_file_url").getOrElse(throw new MissingArgumentException(
          "MANDATORY PARAMETER tree_file_url NOT FOUND IN THE CONFIGURATION XML AT DATASET LEVEL"))
        val datasetTreeFileId = FileDatabase.fileId(datasetId, treeURL, Stage.DOWNLOAD, DatasetInfo.parseFilenameFromURL(treeURL))
        val treeUpdateFailed = failedFiles.map(row => row._1).contains(datasetTreeFileId)
        if (treeUpdateFailed) {
          try {
            /* Failing the update of the tree file, means that any file in this dataset have been updated,
            * so it's necessary to repeat the whole update process. */
            fetchUpdatesForTreeFile(dataset)
            /* call markAsOutdated on the dataset to mark with the status OUTDATED any file which was once in the local
            copy but it's not any more present in the source. Those files have currently the status FILE_STATUS.COMPARE */
            FileDatabase.markAsOutdated(datasetId, Stage.DOWNLOAD)
          } catch {
            case ex: Exception =>
              logger.error("ERROR WHILE FETCHING UPDATES FOR FILES OF DATASET " + dataset.name + ". DETAILS: ", ex)
              markAllFilesAsFailed(datasetId)
          }
        } else {
          // then only some file updates have failed
          // the tree file is necessary for the following steps
          try {
            downloadOrCopyTreeFile(dataset)
            fetchUpdatesForVariants(treeLocalPath.get, dataset)
            fetchUpdatesForMeta(treeLocalPath.get, dataset)
          } catch {
            case ex: Exception =>
              logger.error("ERROR WHILE FETCHING UPDATES FOR FILES OF DATASET " + dataset.name + ". DETAILS: ", ex)
          }
        }
      }
    })
  }

}

