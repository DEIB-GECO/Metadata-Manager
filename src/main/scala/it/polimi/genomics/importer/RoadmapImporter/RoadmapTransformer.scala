package it.polimi.genomics.importer.RoadmapImporter

import java.io._
import java.util.zip.GZIPInputStream

import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.slf4j.LoggerFactory

class RoadmapTransformer  extends GMQLTransformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
    *
    * @param source           source where the files belong to.
    * @param originPath       path for the  "Downloads" folder
    * @param destinationPath  path for the "Transformations" folder
    * @param originalFilename name of the original file .json/.gz
    * @param filename         name of the new file .meta/.bed
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: GMQLSource, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    val fileDownloadPath = originPath + File.separator + originalFilename
    val fileTransformationPath = destinationPath + File.separator + filename
    logger.debug("Start unGzipping: " + originalFilename)
    if (unGzipIt(fileDownloadPath, fileTransformationPath)) {
      logger.info("UnGzipping: " + originalFilename + " DONE")
      true
    }
    else {
      logger.warn("UnGzipping: " + originalFilename + " FAIL")
      false
    }
  }

  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    *
    * @param filename original filename
    * @param dataset  dataset where the file belongs to
    * @param source   source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset: GMQLDataset, source: GMQLSource): List[String] = ???

  /**
    * extracts the gzipFile into the outputPath.
    *
    * @param gzipFile   full location of the gzip
    * @param outputPath full path of destination, filename included.
    */
  def unGzipIt(gzipFile: String, outputPath: String): Boolean = {
    val bufferSize = 1024
    val buffer = new Array[Byte](bufferSize)
    var unGzipped = false
    var timesTried = 0
    while (timesTried < 4 && !unGzipped) {
      try {
        val zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile)))
        val newFile = new File(outputPath)
        val fos = new FileOutputStream(newFile)

        var ze: Int = zis.read(buffer)
        while (ze >= 0) {

          fos.write(buffer, 0, ze)
          ze = zis.read(buffer)
        }
        fos.close()
        zis.close()
        unGzipped = true
      } catch {
        case e: IOException => timesTried += 1
      }
    }
    if (!unGzipped)
      logger.error("Couldnt UnGzip the file: " + outputPath)
    unGzipped
  }

}

