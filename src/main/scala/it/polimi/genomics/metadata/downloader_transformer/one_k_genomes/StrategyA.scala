package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.DatasetFilter.DatasetPattern
import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}   // converts java.util.List into scala.util.List in order to use foreach

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
class StrategyA {

  val localDownloadDir = s"${sys.props.get("user.dir").get}/../Metadata-Manager-WorkDir/MD/"

  def test(treeUrl: String): Unit = {
    val dataset = "REPLACE WITH DATASET OBJECT"
    new FTPHelper().downloadFile(treeUrl, "anonymous", "anonymous") match {
      case None => println("DOWNLOAD FAILED. TODO: IMPLEMENT RETRY STRATEGY")
      case Some(treeLocalPath) => {
        //      2
        val computedHash = FileUtil.md5Hash(treeLocalPath).get
        val savedHash = getSavedTreeHash(dataset)
        if (computedHash != savedHash) {
          //        3
          val remoteDatasetHashes = getRemoteVariantsHashes(treeLocalPath)
          //          TESTED UP TO HERE
          val datasetChanges = compareDatasets(dataset, remoteDatasetHashes)
          if (datasetChanges) {
            updateDataset(remoteDatasetHashes.get)
          }
          //        4
          updateTree(computedHash)
        } else println("LOCAL COPY IS ALREADY UP-TO-DATE")
      }
    }
//    TODO else report error while downloading FTP tree file
  }

  def getRemoteVariantsHashes(treeFilePath: String): Option[List[(String, String)]] = {
    val latestRecords = filterLatestVariantsRecords(treeFilePath)
    val variantsWithHashes = latestRecords.map( record => {
      val parts = PatternMatch.matchParts(record, (new DatasetPattern).get())
//      HERE I RETURN JUST NAME AND HASH, HOWEVER IT'S POSSIBLE TO GET ALSO FILE SIZE AND DATE/TIME INFO
      // return (variant file url, hash)
      (parts.head, parts.last)}
    )
    Some(variantsWithHashes)
  }

  def filterLatestVariantsRecords(treeFilePath: String): List[String] = {
    // TODO adapt to h19
    val latestDatasetDirPath = DatasetFilter.latestVariantsDirPathGRCH38(treeFilePath)
    DatasetFilter.latestVariantsFromDir(latestDatasetDirPath, treeFilePath)
  }

  ///////////////////////////  DRAFT FOR TESTING PURPOSES ONLY    ////////////////////////////////////

  def getSavedTreeHash(dataset: String): String = {
    "5CF1159F6FFD17F9D266B79F839F6929___".toLowerCase
  }

  def compareDatasets(dataset: String, remoteDatasetHashes: Option[List[(String, String)]]) = {
    val hashesFromDatabase = List(("variantFileRemoteURL", "hash"))
    !remoteDatasetHashes.get.forall(hashesFromDatabase.contains(_))
  }

  def updateDataset(newDataset: List[(String, String)]) = {
    // easiest option is to wipe the local dataset and download it again
  }

  def updateTree(treeHash: String) = {
    // update the database with the correct new hash
  }

  /**
   * @param inDataset the dataset containing a parameter with key "files_regex"
   * @return the string value of files_regex if the parameter exists, None otherwise
   */
  def get_files_regex_parameter(inDataset: Dataset): Option[String] = {
    if(inDataset.parameters.exists(_._1 == "files_regex"))
      Some(inDataset.parameters.filter(_._1 == "files_regex").head._2)
    else
      None
  }

  /**
   * @param inDataset the dataset containing a parameter with key "folder_regex"
   * @return the string value of folder_regex, if the parameter exists, None otherwise
   */
  def get_folder_regex_parameter(inDataset: Dataset): Option[String] = {
    if(inDataset.parameters.exists(_._1 == "folder_regex"))
      Some(inDataset.parameters.filter(_._1 == "folder_regex").head._2)
    else
      None
  }
}
