package it.polimi.genomics.metadata.downloader_transformer.finngen

import java.io.{File, _}
import java.security.{DigestInputStream, MessageDigest}

import it.polimi.genomics.metadata.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.FtpDownloader
import it.polimi.genomics.metadata.downloader_transformer.default.utils.Unzipper
import it.polimi.genomics.metadata.step.xml
import it.polimi.genomics.metadata.step.xml.Dataset
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import it.polimi.genomics.metadata.step.xml.Source

import scala.util.Try
import scala.util.matching.Regex


class FinnGenTransformer extends Transformer {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val patternMetadata: Regex = """.*\.gdm\.meta""".r
  val patternRegionData: Regex = """.*\.gdm""".r
  val patternManifest: Regex = """Manifest.*\.tsv""".r
  val manifestName = "ManifestR5.tsv"

  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    * The region file must be before than related meta file
    *
    * @param filename original filename
    * @param dataset  dataset where the file belongs to
    * @param source   source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset: Dataset, source: Source): List[String] = {
    val candidates = new ListBuffer[String]
    filename match {
      case patternManifest() =>
      //do nothing
      case _ =>
        candidates += filename.substring(0, filename.lastIndexOf(".")) + ".gdm"
        candidates += filename.substring(0, filename.lastIndexOf(".")) + ".gdm.meta"
    }
    candidates.toList
  }


  /**
    * recieves .gz files and transform them to get metadata in .meta files and region in .gdm files.
    *
    * @param source           source where the files belong to.
    * @param originPath       path for the  "Downloads" folder
    * @param destinationPath  path for the "Transformations" folder
    * @param originalFilename name of the original file .json/.gz
    * @param filename         name of the new file .meta/.bed
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    var isTransformationDone: Boolean = true //false if an error occurs during the transformation
    val fileDownloadPath = originPath + File.separator + originalFilename
    val fileTransformationPath = destinationPath + File.separator + filename

    filename match {
      case patternMetadata() =>
        val metaGenOutcome = Try(metaGen(source, filename, originPath, destinationPath))
        if (metaGenOutcome.isSuccess) {
          logger.info("metaGen: " + filename + " DONE")
        }
        else {
          isTransformationDone = false
          logger.warn("metaGen: " + filename + " FAILED", metaGenOutcome.failed.get)
        }
      case patternRegionData() =>
        if (originalFilename.endsWith(".gz")) {
          logger.debug("Start unGzipping: " + originalFilename)
          if (Unzipper.unGzipIt(
            fileDownloadPath,
            fileTransformationPath)) {
            logger.info("UnGzipping: " + originalFilename + " DONE")
            true
          }
          else {
            logger.warn("UnGzipping: " + originalFilename + " FAIL")
            false
          }
        } else if (originalFilename.endsWith(".gdm")) {
          System.out.println(fileTransformationPath.toString)
          val file = new File(fileTransformationPath)
          val writer = new PrintWriter(file)
          writer.write("empty")
          writer.close()
        }
        val regionTransfOutcome = Try(regionTransformation(source, filename, originPath, destinationPath))
        if (regionTransfOutcome.isSuccess) {
          logger.info("regionTransform: " + filename + " DONE")
        }
        else {
          isTransformationDone = false
          logger.warn("regionTransform: " + filename + " FAILED", regionTransfOutcome.failed.get)
        }
      case _ =>
        logger.warn(s"File $filename format not supported.")
        isTransformationDone = false
    }
    isTransformationDone
  }

  /**
    * generates the file ".*\.gdm" in the Transformations folder with the region data extracted from the
    * .gz file of the FinnGen dataset.
    *
    * @param source            source where the files belong to
    * @param filename          name of the file to generate
    * @param originPath        path for the  "Downloads" folder
    * @param destinationPath   path for the "Transformations" folder
    */
  def regionTransformation(source: xml.Source, filename: String, originPath: String, destinationPath: String): Unit = {

    //remove the first line containing the names of the columns
    val fileReader = scala.io.Source.fromFile(destinationPath + File.separator + filename)
    val writer = new PrintWriter(destinationPath + File.separator + "tmp.tsv")
    //only 100 rows for testing
    //fileReader.getLines().drop(1).take(100).foreach(line =>{
    fileReader.getLines().drop(1).foreach(line =>{
      var regionLine = new ListBuffer[String]()
      var count = 0
      var chr = line.split("\t")(count)
      val chrom = "chr" + chr
      regionLine += chrom
      count += 1
      val start = line.split("\t")(count)
      regionLine += start
      val stop = (start.toLong + 1).toString
      regionLine += stop
      regionLine += "*"
      count += 1
      while (count < 12){
        regionLine += line.split("\t")(count)
        count += 1
      }
      var index=1
      regionLine.foreach(d => {
        if(index != regionLine.size) writer.write(d+"\t")
        else writer.write(d)
        index += 1
      })
      writer.write("\n")
    })
    writer.close()
    fileReader.close()
    try {
      val df = new File(destinationPath + File.separator + filename)
      val tf = new File(destinationPath + File.separator + "tmp.tsv")
      df.delete
      tf.renameTo(df)
    }
    catch {
      case _: IOException => logger.warn("could not change the file " + destinationPath + File.separator + filename)
    }
    val transformedFile = new File(destinationPath + File.separator + filename)
    val originalName = filename.substring(0, filename.lastIndexOf(".")) + ".gz"
    //val originalName = filename.substring(0, filename.lastIndexOf(".")) + ".gdm"
    val sourceId = FileDatabase.sourceId(source.name)
    val datasetId = FileDatabase.datasetId(sourceId, source.datasets.head.name)
    val filePath = originPath + File.separator + originalName
    val fileId = FileDatabase.fileId(datasetId, filePath, Stage.TRANSFORM, filename)
    val hash = new FtpDownloader().computeHash(filePath)
    FileDatabase.markAsUpdated(fileId, transformedFile.length.toString, hash)
    FileDatabase.getFileNameAndCopyNumber(FileDatabase.fileId(datasetId, filePath, Stage.TRANSFORM, filename))
  }

  /**
    * generates a ".*.gdm.meta" file containing metadata attributes name and value associated to an input file.
    * Each row contains a couple (name \t value).
    *
    * @param source            source where the files belong to
    * @param inPath   path for the  "Downloads" folder
    * @param outPath  path for the "Transformations" folder
    * @param fileName name of the input file for which the .meta file must be generated
    * @return boolean asserting if the meta file is correctly generated
    */
  def metaGen(source: xml.Source, fileName: String, inPath: String, outPath: String): Unit = {
    var manifestReader = scala.io.Source.fromFile(inPath + File.separator + manifestName)
    var manifestHeader = new ListBuffer[String]()
    val writer = new PrintWriter(outPath + File.separator + fileName)

    manifestReader.getLines().take(1).foreach(line =>{
      val params = line.split("\t")
      params.foreach(p => {manifestHeader += p})
    })

    manifestReader = scala.io.Source.fromFile(inPath + File.separator + manifestName)
    manifestReader.getLines().drop(1).foreach(line => {
      val tmp = fileName.split("\\.")(0)
      val bool = line.contains(tmp)
      val endpoint = line.split("\t")(0)
      val traitName = line.split("\t")(1)
      if(bool && tmp.length == endpoint.length) {
        val meta = manifestHeader.zip(line.split("\t"))
        for ( (k,v) <- meta) {
          if (k.contains("name")){ //check if traitName has quotation marks
            val t = v.replaceAll("\"", "")
            writer.write(s"$k\t$t\n")
          } else if (!v.isEmpty) writer.write(s"$k\t$v\n")
        }
      }

    })

    writer.close()
    val transformedFile = new File(outPath + File.separator + fileName)
    val originalName = fileName.split("\\.")(0) + ".gz"
    //val originalName = fileName.split("\\.")(0) + ".gdm"
    val sourceId = FileDatabase.sourceId(source.name)
    val datasetId = FileDatabase.datasetId(sourceId, source.datasets.head.name)
    val filePath = inPath + File.separator + originalName
    val fileId = FileDatabase.fileId(datasetId, filePath, Stage.TRANSFORM, fileName)
    val hash = new FtpDownloader().computeHash(filePath)
    FileDatabase.markAsUpdated(fileId, transformedFile.length.toString, hash)
    FileDatabase.getFileNameAndCopyNumber(FileDatabase.fileId(datasetId, filePath, Stage.TRANSFORM, fileName))

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
      while (dis.read(buffer) != -1) {
      }
    } finally {
      dis.close()
    }

    md5.digest.map("%02x".format(_)).mkString
  }
}