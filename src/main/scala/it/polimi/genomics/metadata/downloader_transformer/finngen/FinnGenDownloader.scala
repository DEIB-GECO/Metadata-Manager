package it.polimi.genomics.metadata.downloader_transformer.finngen

import java.io.{File, FileInputStream, FileWriter, PrintWriter}
import java.net.URL
import java.security.{DigestInputStream, MessageDigest}

import scala.sys.process._
import it.polimi.genomics.metadata.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer.Downloader
import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.step.xml.Source
import org.slf4j.{Logger, LoggerFactory}


class FinnGenDownloader extends Downloader{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  /**
    * downloads the files from the source defined in the information
    * into the folder defined in the source and its dataset
    *
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    */
  override def download(source: Source, parallelExecution: Boolean): Unit = {
    if (source.downloadEnabled) {
      logger.info("Starting download for: " + source.name)
      if (!new java.io.File(source.outputFolder).exists) {
        new java.io.File(source.outputFolder).mkdirs()
      }
      val sourceId = FileDatabase.sourceId(source.name)
      source.datasets.foreach(dataset => {
        if (dataset.downloadEnabled) {

          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
          if (!new File(outputPath).exists())
            new File(outputPath).mkdirs()

          source.datasets.foreach(dataset => {
            if (dataset.downloadEnabled) {
              val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
              FileDatabase.markAsOutdated(datasetId, Stage.DOWNLOAD)
              getManifest(source.url, source, outputPath, datasetId, dataset)
            }
          })
        }
      })
      logger.info(s"Download for ${source.name} Finished.")
    }
  }


  /**
    * downloads the manifest of the release #3 of FinnGne. From the manifest are extracted
    * the names of the files to download.
    *
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    * @param url url of the manifest to be downlaoded.
    * @param path output path of the manifest.
    * @param datasetId id of the dataset in the server.
    */
  def getManifest(url: String, source: Source, path: String, datasetId: Int, dataset: Dataset): Boolean ={
    var manifestName = "ManifestR5.tsv"
    val manifestPath = path + File.separator + manifestName
    try {
      val src = scala.io.Source.fromURL(url)
      val out = new java.io.FileWriter(manifestPath)
      out.write(src.mkString)
      out.close()
      if (new File(manifestPath).exists()) {
        logger.info(s"Downloading manifest: " + manifestPath + " from: " + url + " DONE")

        //set the copyNumber to 1
        FileDatabase.getFileNameAndCopyNumber(FileDatabase.fileId(datasetId, url, Stage.DOWNLOAD, manifestName))

        val fileId = FileDatabase.fileId(datasetId, url, Stage.DOWNLOAD, manifestName)
        val hash = computeHash(manifestPath)
        FileDatabase.markAsUpdated(fileId, new File(manifestPath).length.toString, hash)

        downloadFiles(path, manifestPath, datasetId, dataset)
      }
      true
    }
      catch {
        case e: Throwable =>
          logger.error("Downloading: " + manifestPath + " from: " + url + " failed: ")
          false
      }
  }


  /**
    * downloads the files which links are obtained from the manifest.tsv
    *
    * @param manifestPath
    * @param datasetId id of the dataset in the server.
    */
  def downloadFiles(path: String, manifestPath: String, datasetId: Int, dataset: Dataset): Unit ={
    var readerManifest = scala.io.Source.fromFile(manifestPath)
    val total = readerManifest.getLines().length - 1
    var counter = 0
    readerManifest = scala.io.Source.fromFile(manifestPath)
    //readerManifest.getLines().drop(1).take(20).foreach(line => {
    readerManifest.getLines().drop(1).foreach(line => {

      val fileTrait = line.split("\t")(1)
      if (!fileTrait.equals("")) {
        val fileUrl = line.split("\t")(6)
        val fileName = line.split("\t")(0) + ".gz"
        //val fileName = line.split("\t")(0) + ".gdm"
        val filePath = path + File.separator + fileName
        if (fileName == "F5_SCHIZOTYP.gz" || fileName == "F5_SCHIZOAFF.gz" ||
           fileName == "KRA_PSY_SCHIZODEL.gz" || fileName == "G6_ALZHEIMER_INCLAVO.gz" ||
           fileName == "KRA_PSY_SCHIZODEL_EXMORE.gz" || fileName == "F5_ALZHDEMENT.gz" ||
           fileName == "G6_ALZHEIMER_EXMORE.gz" || fileName == "G6_ALZHEIMER.gz" || fileName == "F5_SCHIZO.gz"){


        FileDatabase.getFileNameAndCopyNumber(FileDatabase.fileId(datasetId, fileUrl, Stage.DOWNLOAD, fileName))

        try {

          //the file is downloaded directly inside the File object
          new URL(fileUrl) #> new File(filePath) !!
          //val file = new File(filePath)
          //val writer = new PrintWriter(file)
          //writer.write("empty")
          //writer.close()

          counter = counter + 1

          if (new File(filePath).exists()) {
            logger.info(s"Downloading [$counter/$total]: " + filePath + " from: " + fileUrl + " DONE")
            val fileId = FileDatabase.fileId(datasetId, fileUrl, Stage.DOWNLOAD, fileName)
            val hash = computeHash(filePath)
            FileDatabase.markAsUpdated(fileId, new File(filePath).length.toString, hash)
            true
          }
          else {
            logger.error("Downloading: " + filePath + " from: " + fileUrl + " failed: ")
            false
          }

        } catch {
          case e: Throwable =>
            logger.error("Downloading: " + filePath + " from: " + fileUrl + " failed. ")
            false
        }}
      }
    })

    //+1 is for the manifest which is out from this counters
    FileDatabase.runDatasetDownloadAppend(datasetId, dataset, total+1, counter+1)
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
      while (dis.read(buffer) != -1) {
      }
    } finally {
      dis.close()
    }

    md5.digest.map("%02x".format(_)).mkString
  }
}

