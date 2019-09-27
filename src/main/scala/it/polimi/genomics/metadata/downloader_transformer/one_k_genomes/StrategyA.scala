package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.nio.file.{Files, Paths}

import it.polimi.genomics.metadata.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer.Downloader
import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.DatasetFilter.DatasetPattern
import it.polimi.genomics.metadata.step.xml
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Created by Tom on set, 2019
 *
 * This class implements the preferred strategy to download and check updates for 1kGenomes datasets:
 * 1. Download a file describing the FTP tree structure
 * 2. Check against changes.
 * 3. If there're changes, determine of they refer to the files of interest for the dataset. If yes, scan the file to
 * understand if there're updated versions of the files already own, eventually download them and update the local copy
 * 3.1. Update the local copy of the FTP tree structure to the latest version and quit.
 */
class StrategyA extends Downloader {

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
    logger.info("Starting download for: " + source.name)
    val sourceId = FileDatabase.sourceId(source.name)
    source.datasets.foreach(dataset => {
      if (dataset.downloadEnabled) {
        val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
        // mark with the temporary status FILE_STATUS.COMPARE any file in the dataset
        FileDatabase.markToCompare(datasetId, Stage.DOWNLOAD)
        // download and compare local vs source versions of this same dataset
        try {
          fetchUpdatesForTreeFile(dataset)
          /* call markAsOutdated on the dataset to mark with the status OUTDATED any file which was once in the local
          copy but it's not any more present in the source */
          FileDatabase.markAsOutdated(datasetId, Stage.DOWNLOAD)
        } catch {
          case ex: Exception =>
            println("ERROR WHILE FETCHING UPDATES FOR VARIANT FILES OF DATASET " + dataset.name + ". DETAILS:")
            ex.printStackTrace()
            /* mark as failed at least the tree file even if it's updated to trigger the update of the whole dataset on
          the next run */
            markAllFilesAsFailed(datasetId)
        }
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
    treeURL = dataset.getParameter("tree_file_url").getOrElse(throw new NullPointerException(
      "REQUIRED PARAMETER \"tree_file_url\" MISSING FROM THE CONFIGURATION XML AT DATASET LEVEL"))
    //    1
    val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
    downloadOrCopyTreeFile(dataset)
    //      2
    val computedHash = FileUtil.md5Hash(treeLocalPath.get).get
    //      3
    val fileId = FileDatabase.fileId(datasetId, treeURL, Stage.DOWNLOAD, DatasetFilter.parseFilenameFromURL(treeURL))
    // Hash is enough for comparing different versions of the file, so I left size and timestamp args blank
    if (!FileDatabase.checkIfUpdateFile(fileId, computedHash, "", ""))
      markAllFilesAsUpdated(datasetId)
    else {
      // update the dataset files...
      fetchUpdatesForVariants(treeLocalPath.get, dataset)
      //    3.1
      // then update the tree file
      /* Actually I already downloaded the tree file overwriting the old local copy if it was present, so
      I just need to notify it to the database. */
      FileDatabase.markAsUpdated(fileId, "", computedHash)
    }
  }

  /**
   * Parses the tree file, searches for updated versions of the given dataset, download the updates and updates the
   * database.
   *
   * @param treeLocalPath path to the local tree file
   * @param dataset the dataset whom variants will be updated
   * @throws IllegalStateException if the parsing of the tree file returns an empty set of variants.
   */
  private def fetchUpdatesForVariants(treeLocalPath: String, dataset: Dataset): Unit = {
    //        3
    val variantRecords = getRemoteVariantRecords(treeLocalPath, dataset)
    if (variantRecords.isEmpty) {
      throw new IllegalStateException("DOWNLOAD CAN'T CONTINUE: UNABLE TO EXTRACT VARIANT SET FROM TREE LOCAL FILE")
    } else {
      val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
      val urlPrefix = getURLPrefix(dataset)
      variantRecords.foreach(record => {
        val filename = DatasetFilter.parseFilenameFromURL(filePath = record._1)
        val fileId = FileDatabase.fileId(datasetId, url = record._1, Stage.DOWNLOAD, filename)
        if (FileDatabase.checkIfUpdateFile(fileId, hash = record._4, originSize = record._2, originLastUpdate = record._3)) {
          // download the file at proper location
          new FTPHelper(dataset).downloadFile(url = s"$urlPrefix${record._1}") match {
            case Failure(exception) =>
              println("DOWNLOAD OF VARIANT FAILED. DETAILS: ")
              exception.printStackTrace()
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
   * Given a tree file path, this method filters the records, selects the most recent version available of a dataset,
   * then it parses the attributes present in the tree file, namely the file path, kind, size, timestamp and hash.
   *
   * @param treeFilePath the path to the location of the tree file on the local filesystem
   * @return a list of tuples, one for each record, containing in order: file path, size and timestamp as Strings. If the
   */
  def getRemoteVariantRecords(treeFilePath: String, dataset: Dataset): List[(String, String, String, String)] = {
    def filterLatestVariantsRecords(treeFilePath: String, dataset: Dataset): List[String] = {
      val baseRemoteDatasetDir = dataset.getParameter("dataset_remote_base_directory").getOrElse(
        throw new NullPointerException("MISSING REQUIRED PARAMETER dataset_remote_base_directory AT DATASET LEVEL IN XML CONFIG FILE"))
      // select the subdir containing the latest version of this dataset
      val latestDatasetDirPath = DatasetFilter.latestVariantSubdirectory(baseRemoteDatasetDir, treeFilePath)
      // select the variant files within
      DatasetFilter.variantsFromDir(latestDatasetDirPath, treeFilePath)
    }

    val latestRecords = filterLatestVariantsRecords(treeFilePath, dataset)
    if(latestRecords.isEmpty)
      throw new IllegalArgumentException("NO VARIANTS FOUND. PLEASE REVIEW THE DATASET PARAMETER dataset_remote_base_directory OF YOUR XML CONFIG FILE")
    val variantsWithHashes = latestRecords.map( record => {
      val parts = PatternMatch.matchParts(record, (new DatasetPattern).get())   // the pattern used affects the position and kind of info obtained
      // here is possible to return any of the field that populate the tree. I return file path, size, timestamp string and the hash.
      (parts.head, parts(2), parts(3), parts.last)
    })
    variantsWithHashes
  }

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
      val newPath = s"${DatasetFilter.getDownloadDir(dataset)}${FileUtil.getFileNameFromPath(treeLocalPath.get)}"
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

  def getURLPrefix(dataset: Dataset): String = {
    dataset.getParameter("url_prefix_tree_file_records").getOrElse(
      throw new NullPointerException("REQUIRED PARAMETER url_prefix_tree_file_records IS MISSING FROM XML CONFIG FILE")
    )
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
  override def downloadFailedFiles(source: Source, parallelExecution: Boolean): Unit = ???
}

