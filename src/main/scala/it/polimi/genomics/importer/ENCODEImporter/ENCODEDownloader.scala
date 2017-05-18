package it.polimi.genomics.importer.ENCODEImporter

import java.io.{File, FileInputStream}
import java.net.URL
import java.security.{DigestInputStream, MessageDigest}

import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLDownloader, GMQLSource}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.xml.{Elem, XML}

/**
  * Created by Nacho on 10/13/16.
  */
class ENCODEDownloader extends GMQLDownloader {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * downloads the files from the source defined in the loader
    * into the folder defined in the loader
    * recursively checks all folders and subfolders matching with the regular expressions defined in the loader
    *
    * @param source contains specific download and sorting info.
    */
  override def download(source: GMQLSource): Unit = {
    logger.info("Starting download for: " + source.name)
    if (!new java.io.File(source.outputFolder).exists) {
      logger.debug("file " + source.outputFolder + " created")
      new java.io.File(source.outputFolder).mkdirs()
    }
    downloadIndexAndMeta(source)
    downloadFailedFiles(source)
  }

/**
    * downloads the failed files from the source defined in the loader
    * into the folder defined in the loader
    *
    * For each dataset, download method should put the downloaded files inside
    * /source.outputFolder/dataset.outputFolder/Downloads
    *
    * @param source contains specific download and sorting info.
    */
  override def downloadFailedFiles(source: GMQLSource): Unit = {
    logger.info(s"Downloading failed files for source ${source.name}")
    val sourceId = FileDatabase.sourceId(source.name)
    source.datasets.foreach(dataset =>{
      logger.info(s"Downloading failed files for dataset ${dataset.name}")
      val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
      val failedFiles = FileDatabase.getFailedFiles(datasetId,STAGE.DOWNLOAD)
      var counter = 0
      val totalFiles = failedFiles.size
      failedFiles.foreach(file=>{
        //in file I have (fileId,name,copyNumber,url, hash)
        val filename =
          if (file._3 == 1) file._2
          else file._2.replaceFirst("\\.", "_" + file._3 + ".")
        val filePath =
          source.outputFolder + File.separator + dataset.outputFolder +
            File.separator + "Downloads" + File.separator + filename
        if (urlExists(file._4)) {
          var downloaded = downloadFileFromURL(file._4, filePath, counter+1, totalFiles)
          var timesTried = 0
          val downloadedFile = new File(filePath)
          var hash = computeHash(filePath)
          while ((hash != file._5 && !filename.endsWith(".json")) && timesTried < 4 && !downloaded) {
            downloaded = downloadFileFromURL(file._4, filePath, counter+1, totalFiles)
            hash = computeHash(filePath)
            timesTried += 1
          }
          if (timesTried == 4)
            FileDatabase.markAsFailed(file._1)
          else {
            FileDatabase.markAsUpdated(file._1, downloadedFile.length.toString)
          }
          counter = counter + 1
        }
        else{
          logger.info(s"${file._4} does not exist or no internet connection is enabled.")
        }
        FileDatabase.runDatasetDownloadAppend(datasetId, dataset, 0, counter)
      })
    })
  }

  /**
    * From: http://stackoverflow.com/questions/41642595/scala-file-hashing
    * calculates the md5 hash for a file.
    *
    * @param path file location
    * @return hash code
    */
  def computeHash(path: String): String = {
    val buffer = new Array[Byte](8192)
    val md5 = MessageDigest.getInstance("MD5")

    val dis = new DigestInputStream(new FileInputStream(new File(path)), md5)
    try {
      while (dis.read(buffer) != -1) {}
    } finally {
      dis.close()
    }

    md5.digest.map("%02x".format(_)).mkString
  }

  /**
    * For ENCODE given the parameters, a link for downloading metadata and
    * file index is generated, here, that file is downloaded and then, for everyone
    * downloads all the files linked by it.
    *
    * @param source information needed for downloading ENCODE datasets.
    */
  private def downloadIndexAndMeta(source: GMQLSource): Unit = {
    source.datasets.foreach(dataset => {
      if (dataset.downloadEnabled) {
        val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(source.name), dataset.name)
        val stage = STAGE.DOWNLOAD
        val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
        if (!new java.io.File(outputPath).exists) {
          new java.io.File(outputPath).mkdirs()
        }
        val indexAndMetaUrl = generateDownloadIndexAndMetaUrl(source, dataset)
        //ENCODE always provides the last version of this .meta file.
        if (urlExists(indexAndMetaUrl)) {
          /*I will check all the server files against the local ones so i mark as to compare,
            * the files will change their state while I check each one of them. If there is
            * a file deleted from the server will be marked as OUTDATED before saving the table back*/
          FileDatabase.markToCompare(datasetId, stage)

          val metadataCandidateName = "metadata.tsv"
          var downloaded = downloadFileFromURL(
            indexAndMetaUrl,
            outputPath + File.separator + metadataCandidateName, 1, 1)
          var timesTried = 0
          while (!downloaded && timesTried < 4) {
            downloaded = downloadFileFromURL(
                indexAndMetaUrl,
                outputPath + File.separator + metadataCandidateName, 1, 1)
            timesTried += 1
          }
          val filePath = outputPath + File.separator + metadataCandidateName
          val file = new File(filePath)
          val fileId = FileDatabase.fileId(datasetId, indexAndMetaUrl, stage, metadataCandidateName)
          FileDatabase.getFileNameAndCopyNumber(fileId)

          if (downloaded && file.exists()) {
            val hash = computeHash(filePath)
            FileDatabase.checkIfUpdateFile(fileId, hash, file.length.toString, DateTime.now.toString)
            FileDatabase.markAsUpdated(fileId, file.length.toString)
            downloadFilesFromMetadataFile(source, dataset)
            logger.info("download for " + dataset.outputFolder + " completed")
          }
          else {
            FileDatabase.markAsFailed(fileId)
            logger.warn("couldn't download metadata.tsv file")
          }
          FileDatabase.markAsOutdated(datasetId, stage)
        }
        else {
          logger.error("download link generated by " + dataset.outputFolder + " does not exist")
          logger.debug("download link:" + indexAndMetaUrl)
        }
      }
    })
  }

  /**
    * generates download link for the metadata file
    *
    * @param source  contains information related for connecting to ENCODE
    * @param dataset contains information for parameters of the url
    * @return full url to download metadata file from encode.
    */
  def generateDownloadIndexAndMetaUrl(source: GMQLSource, dataset: GMQLDataset): String = {
    source.url + source.parameters.filter(_._1.equalsIgnoreCase("metadata_prefix")).head._2 + generateParameterSet(dataset) + source.parameters.filter(_._1 == "metadata_suffix").head._2
  }

  /*def generateReportUrl(source: GMQLSource, dataset: GMQLDataset): String ={
    source.url + source.parameters.filter(_._1.equalsIgnoreCase("report_prefix")).head._2 + generateParameterSet(dataset) + "&" + generateFieldSet(source)
  }*/
  def generateFieldSet(source: GMQLSource): String = {
    val file: Elem = XML.loadFile(source.parameters.filter(_._1.equalsIgnoreCase("encode_metadata_configuration")).head._2)
    var set = ""
    ((file \\ "encode_metadata_config" \ "parameter_list").filter(list =>
      (list \ "@name").text.equalsIgnoreCase("encode_report_tsv")) \ "parameter").filter(field =>
      (field \ "@include").text.equalsIgnoreCase("true") && (field \ "key").text.equalsIgnoreCase("field")).foreach(field => {
      set = set + (field \\ "key").text + "=" + (field \\ "value").text + "&"
    })
    if (set.endsWith("&"))
      set.substring(0, set.length - 1)
    else
      set
  }

  /**
    * concatenates all the folder's parameters with & in between them
    * and = inside them
    *
    * @param dataset contains all the parameters information
    * @return string with parameter=value & ....
    */
  private def generateParameterSet(dataset: GMQLDataset): String = {
    var set = ""
    dataset.parameters.foreach(parameter => {
      set = set + parameter._1 + "=" + parameter._2 + "&"
    })
    if (set.endsWith("&"))
      set.substring(0, set.length - 1)
    else
      set
  }

  /**
    * given a url and destination path, downloads that file into the path
    *
    * @param url  source file url.
    * @param path destination file path and name.
    */
  def downloadFileFromURL(url: String, path: String, number: Int, total: Int): Boolean = {
    try {
      new URL(url) #> new File(path) !!;
      logger.info(s"Downloading [$number/$total]: " + path + " from: " + url + " DONE")
      true
    }
    catch {
      case e: Throwable =>
        logger.error("Downloading: " + path + " from: " + url + " failed: ")
        false
    }
  }

  /**
    * explores the downloaded metadata file with all the urls directing to the files to download,
    * checks if the files have to be updated, downloaded, deleted and performs the actions needed.
    * puts all downloaded files into /information.outputFolder/folder.outputFolder/Downloads
    *
    * @param source  contains information for ENCODE download.
    * @param dataset dataset specific information about its location.
    */
  private def downloadFilesFromMetadataFile(source: GMQLSource, dataset: GMQLDataset): Unit = {
    //attributes that im looking into the line:
    //Experiment date released (22), Size (36), md5sum (38), File download URL(39)
    //maybe this parameters should be entered by xml file
    val path = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
    val file = Source.fromFile(path + File.separator + "metadata" + ".tsv")
    if (file.hasNext) {
      val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(source.name), dataset.name)
      val stage = STAGE.DOWNLOAD

      val header = file.getLines().next().split("\t")

      val originLastUpdate = header.lastIndexOf("Experiment date released")
      val originSize = header.lastIndexOf("Size")
      val experimentAccession = header.lastIndexOf("Experiment accession")
      //to be used
      val md5sum = header.lastIndexOf("md5sum")
      val url = header.lastIndexOf("File download URL")
      var counter = 0
      var downloadedFiles = 0
      //add if json metadata *2, is metadata.tsv *1
      val total = Source.fromFile(path + File.separator + "metadata.tsv").getLines().filterNot(_ == "").drop(1).length * 2
      Source.fromFile(path + File.separator + "metadata.tsv").getLines().filterNot(_ == "").drop(1).foreach(line => {
        val fields = line.split("\t")
        val candidateName = fields(url).split(File.separator).last
        val fileId = FileDatabase.fileId(datasetId, fields(url), stage, candidateName)
        val fileNameAndCopyNumber = FileDatabase.getFileNameAndCopyNumber(fileId)

        val filename =
          if (fileNameAndCopyNumber._2 == 1) fileNameAndCopyNumber._1
          else fileNameAndCopyNumber._1.replaceFirst("\\.", "_" + fileNameAndCopyNumber._2 + ".")

        val filePath = path + File.separator + filename

        val candidateNameJson = fields(url).split(File.separator).last + ".json"
        val urlExperimentJson = source.url + "experiments" + File.separator + fields(experimentAccession) + File.separator + "?frame=embedded&format=json"
        val fileIdJson = FileDatabase.fileId(datasetId, urlExperimentJson, stage, candidateNameJson)
        FileDatabase.checkIfUpdateFile(fileIdJson, fields(md5sum), fields(originSize), fields(originLastUpdate))

        if (FileDatabase.checkIfUpdateFile(fileId, fields(md5sum), fields(originSize), fields(originLastUpdate))) {
          //this is the region data part.
          if (urlExists(fields(url))) {
            var downloaded = downloadFileFromURL(fields(url), filePath, counter + 1, total)
            val file = new File(filePath)
            var hash = computeHash(filePath)

            var timesTried = 0

            while ((!downloaded || hash != fields(md5sum)) && timesTried < 4) {
              downloaded = downloadFileFromURL(fields(url), filePath, counter + 1, total)
              hash = computeHash(filePath)
              timesTried += 1
            }
            if (timesTried == 4)
              FileDatabase.markAsFailed(fileId)
            else {
              FileDatabase.markAsUpdated(fileId, file.length.toString)
              downloadedFiles = downloadedFiles + 1
            }
            counter = counter + 1


            //this is the metadata part.
            //example of json url https://www.encodeproject.org/experiments/ENCSR570HXV/?frame=embedded&format=json
            //            val urlExperimentJson = source.url + "experiments" + File.separator + fields(experimentAccession) + File.separator + "?frame=embedded&format=json"
            //have to implement if metadata is from json, else do not download (include just metadata.tsv metadata)
            if (urlExists(urlExperimentJson)) {
              //              val candidateNameJson = fields(url).split(File.separator).last + ".json"
              //              val fileIdJson = FileDatabase.fileId(datasetId, urlExperimentJson, stage, candidateNameJson)
              val fileNameAndCopyNumberJson = FileDatabase.getFileNameAndCopyNumber(fileIdJson)
              val jsonName =
                if (fileNameAndCopyNumberJson._2 == 1) fileNameAndCopyNumberJson._1
                else fileNameAndCopyNumberJson._1.replaceFirst("\\.", "_" + fileNameAndCopyNumberJson._2 + ".")

              val filePathJson = path + File.separator + jsonName
              //As I dont have the metadata for the json file i use the same as the region data.
              //              if(FileDatabase.checkIfUpdateFile(fileIdJson,fields(md5sum),fields(originSize),fields(originLastUpdate))){
              //              FileDatabase.checkIfUpdateFile(fileIdJson, fields(md5sum), fields(originSize), fields(originLastUpdate))
              downloaded = downloadFileFromURL(urlExperimentJson, filePathJson, counter +1, total)
              timesTried = 0
              while (!downloaded && timesTried < 4) {
                downloaded = downloadFileFromURL(urlExperimentJson, filePathJson, counter +1, total)
                timesTried += 1
              }
              val file = new File(filePathJson)
              //cannot check the correctness of the download for the json.
              if(downloaded) {
                FileDatabase.markAsUpdated(fileIdJson, file.length.toString)
                downloadedFiles = downloadedFiles + 1
              }
              else
                FileDatabase.markAsFailed(fileIdJson)
              // if metadata is from json add this other +1.
              counter = counter + 1
              //              }
            }
          }
          else {
            FileDatabase.markAsFailed(fileId)
            logger.error("could not download " + fields(url) + "path does not exist")
          }
        }
        else
          logger.info(s"File ${fields(url)} is already up to date.")
      })
      FileDatabase.runDatasetDownloadAppend(datasetId, dataset, total, counter)
      if (total == downloadedFiles) {
        //add successful message to database.
        logger.info(s"All $total files for dataset ${dataset.name} of source ${source.name} downloaded correctly.")
      }
      else {
        //add the warning message to the database
        logger.warn(s"Dataset ${dataset.name} of source ${source.name} downloaded $downloadedFiles/$total files.")
      }
    }
    else
      logger.debug("metadata.tsv file is empty")
  }

  /**
    * checks if the given URL exists
    *
    * @param path URL to check
    * @return URL exists
    */
  def urlExists(path: String): Boolean = {
    try {
      scala.io.Source.fromURL(path)
      true
    } catch {
      case _: Throwable =>
        try {
          //here I check if the internet connection was lost.
          scala.io.Source.fromURL("www.google.com")
          //if cannot connect to google neither the url, I assume internet connection was lost
          false
        }
        catch {
          case _: Throwable =>
            //here means there is no internet connection, have to wait and try again.
            logger.warn("Seems internet connection was lost, retrying in 5 minutes")
            Thread.sleep(5 * 60 * 1000)
            urlExists(path)
        }
    }
  }
}
