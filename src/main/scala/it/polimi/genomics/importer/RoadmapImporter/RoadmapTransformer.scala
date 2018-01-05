package it.polimi.genomics.importer.RoadmapImporter

import java.io._
import java.util.zip.GZIPInputStream

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
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
    if (originalFilename.endsWith(".gz")) {
      logger.debug("Start unGzipping: " + originalFilename)
      if (unGzipIt(fileDownloadPath, fileTransformationPath)) {
        logger.info("UnGzipping: " + originalFilename + " DONE")
        metaGen(filename, originPath, destinationPath)
        true
      }
      else
      {
        logger.warn("UnGzipping: " + originalFilename + " FAIL")
        false
      }
    }
    else if(originalFilename.endsWith(".csv")) {
      true
    }
    else false
  }

  def metaGen(fileName: String, inPath: String, outPath: String) = {
    val sheet1: File = new File(inPath + File.separator + "jul2013.roadmapData.qc_Consolidated_EpigenomeIDs_summary_Table.csv")
    val sheet2: File = new File(inPath + File.separator + "jul2013.roadmapData.qc_Consolidated_EpigenomeIDs_QC.csv")
    val tempSheet: File = new File(inPath + File.separator + "tempSheet.csv")
    val reader1: CSVReader = CSVReader.open(sheet1)
    val firstSheet: List[List[String]] = reader1.all()
    val newHeader: List[String] = (firstSheet(0) zip firstSheet(1)).map(tuple => {
      if(firstSheet(0).count(_ == tuple._1)>1 && tuple._2.nonEmpty)
        tuple._1+ ":" + tuple._2
      else
        tuple._1})
    val writer: CSVWriter = CSVWriter.open(tempSheet)
    writer.writeRow(newHeader)
    writer.writeAll(firstSheet)
    writer.close()
    reader1.close()
    val reader3 = CSVReader.open(tempSheet)
    val reader2: CSVReader = CSVReader.open(sheet2)
    val listMaps1: List[Map[String,  String]] = reader3.allWithHeaders()
    val listMaps2: List[Map[String,  String]] = reader2.allWithHeaders()
    val mark: String = extractMark(fileName)
    val eid: String = fileName.split("-")(0)
    val eid_index: Int = listMaps1.indexWhere(_.get("Epigenome ID (EID)").get == eid)
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outPath + File.separator + fileName + ".meta"))))) {
      writer => {
        mapToFile(listMaps1(eid_index), writer)
        for (m <- listMaps2)
          if(m.get("EID").get == listMaps1(eid_index).get("Epigenome ID (EID)").get && m.get("MARK").get == mark)
            mapToFile(m, writer)
      }
    }
    reader2.close()
    reader3.close()
    tempSheet.delete
  }

  def extractMark(fileName: String): String = {
    val core: String = fileName.split("-")(1).split("\\.").dropRight(1).mkString(".")
    if(core == "DNase")
      core.split("\\.")(0)
    else
      core
  }

  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try { block(resource) }
    finally { resource.close() }
  }

  def mapToFile(map: Map[String, String], writer: Writer): Unit = {
    for ((k, v) <- map.toSeq.sortBy(_._1))
      if(!(k.isEmpty || v.isEmpty)) writer.write(s"${k.filterNot("\n".toSet)
        .replaceAll("\\s+$", "")
        .replace(" ","_")}\t$v\n")
  }
  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    *
    * @param filename original filename
    * @param dataset  dataset where the file belongs to
    * @param source   source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset: GMQLDataset, source: GMQLSource): List[String] = {
    List[String](filename.substring(0, filename.lastIndexOf(".")))
  }

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

