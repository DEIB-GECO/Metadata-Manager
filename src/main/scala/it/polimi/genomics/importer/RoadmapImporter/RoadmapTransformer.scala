package it.polimi.genomics.importer.RoadmapImporter

import java.io._
import java.util.regex.Pattern

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import it.polimi.genomics.importer.DefaultImporter.utils.Unzipper
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.slf4j.{Logger, LoggerFactory}

class RoadmapTransformer  extends GMQLTransformer {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * recieves .gz files and transform them invalid regions files and generate the corresponding .meta files
    *
    * @param source           source where the files belong to.
    * @param originPath       path for the  "Downloads" folder
    * @param destinationPath  path for the "Transformations" folder
    * @param originalFilename name of the original file
    * @param filename         name of the new file
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: GMQLSource, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    val fileDownloadPath = originPath + File.separator + originalFilename
    val fileTransformationPath = destinationPath + File.separator + filename
    val splitPath = destinationPath.split(Pattern.quote(File.separator))
    val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(source.name), splitPath(splitPath.length-2))

    val stage = STAGE.DOWNLOAD
    if (originalFilename.endsWith(".gz")) {
      logger.debug("Start unGzipping: " + originalFilename)
      if (Unzipper.unGzipIt(fileDownloadPath, fileTransformationPath)) {
        logger.info("UnGzipping: " + originalFilename + " DONE")
        if (metaGen(filename, originPath, destinationPath)) {
          logger.info("metaGen: " + originalFilename + " DONE")
          val fileId = FileDatabase.fileId(datasetId, "", stage, filename+".meta")
          FileDatabase.markAsUpdated(fileId, new File(destinationPath + File.separator + filename+".meta").length.toString)
          true
        }
        else {
          logger.warn("metaGen: " + originalFilename + " FAIL")
          false
        }
      }
      else {
        logger.warn("UnGzipping: " + originalFilename + " FAIL")
        false
      }
    }
    else false
  }

  /**
    * generate a .meta file in tsv format containing metadata attributes name and value associated to an input file
    *
    * @param inPath   path for the  "Downloads" folder
    * @param outPath  path for the "Transformations" folder
    * @param fileName name of the input file for which the .meta file must be generated
    * @return boolean asserting if the meta file is correctly generated
    */
  def metaGen(fileName: String, inPath: String, outPath: String): Boolean = {
    try {
      val sheet1: File = new File(inPath + File.separator + "jul2013.roadmapData.qc_Consolidated_EpigenomeIDs_summary_Table.csv")
      val sheet2: File = new File(inPath + File.separator + "jul2013.roadmapData.qc_Consolidated_EpigenomeIDs_QC.csv")
      val tempSheet: File = new File(inPath + File.separator + "tempSheet.csv")
      val reader1: CSVReader = CSVReader.open(sheet1)

      //load first sheet in memory
      val firstSheet: List[List[String]] = reader1.all()

      //merge first line (header) with the second one (sub-header) of the first sheet
      val newHeader: List[String] = (firstSheet.head zip firstSheet(1)).map(tuple => {
        if (firstSheet.head.count(_ == tuple._1) > 1 && tuple._2.nonEmpty)
          tuple._1 + "__" + tuple._2
        else
          tuple._1
      })

      //write in a temporary file the new header and the first sheet
      val writer: CSVWriter = CSVWriter.open(tempSheet)
      writer.writeRow(newHeader)
      writer.writeAll(firstSheet)
      writer.close()
      reader1.close()

      //load in memory temp sheet and second sheet
      val reader3: CSVReader = CSVReader.open(tempSheet)
      val reader2: CSVReader = CSVReader.open(sheet2)

      //load the headers
      val listMaps1: List[Map[String, String]] = reader3.allWithHeaders()
      val listMaps2: List[Map[String, String]] = reader2.allWithHeaders()

      //extract mark, eid and format from the file name
      val mark: String = extractMark(fileName)
      val eid: String = fileName.split("-")(0)
      val format: String = extractFormat(fileName)
      //select line in first sheet corresponding to the input file
      val eid_index: Int = listMaps1.indexWhere(_ ("Epigenome ID (EID)") == eid)

      //write attribute name and value on .meta file
      using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outPath + File.separator + fileName + ".meta"))))) {
        writer => {
          mapToFile(listMaps1(eid_index).filterKeys(key => !(key.split("__").length > 1 && key.split("__")(1) != mark)), writer, keyPrefix =  "epi")
          for (m <- listMaps2)
            if (m("EID") == listMaps1(eid_index)("Epigenome ID (EID)") && m("MARK") == mark)
              mapToFile(m, writer, keyPrefix =  "exp")
          writer.write(s"manually_curated__format\t$format")
        }
      }
      reader2.close()
      reader3.close()
      tempSheet.delete
      true
    } catch {
      case e: Exception =>
        logger.debug(s"exeption occurred during $fileName metadata generation", e)
        false
    }
  }

  /**
    * extract the mark from a Roadmap consolidated peak file
    *
    * @param fileName         name of the new file
    * @return String containing the mark in the file name
    */
  def extractMark(fileName: String): String = {
    //val core: String = fileName.split("-")(1).split("\\.").dropRight(1).mkString(".")
    val core = fileName.split("-")(1).split("\\.")
    if(core(0) == "H2A")
      core(0) + "0" + core(1)
    else
      core(0)
  }

  /**
    * extract the format from a Roadmap consolidated peak file
    *
    * @param fileName         name of the new file
    * @return String containing the format in the file name
    */
  def extractFormat(fileName: String): String = {
    val nameComp = fileName.split("-")(1).split("\\.")
    var format: String = nameComp(nameComp.length-1)
    if (nameComp.length > 1 && format == "bed")
      format += "_" + nameComp(nameComp.length-2)
    if (format == "bed_peaks" || format == "bed_broad") {
      if(nameComp.length > 2 && nameComp(nameComp.length-3) == "all")
        format += "_all"
      else
        format += "_fdr"
    }
    format
  }

  /**
    * perform an operation over a resources and close it
    *
    * @param resource        a closeable resources to handle
    * @param block           an operation to perform over the resource
    * @return closed resources
    */
  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try { block(resource) }
    finally { resource.close() }
  }

  /**
    * write a map over a writable resources in tsv; each line contains a key-value pair
    *
    * @param map              map to write
    * @param writer           resources where the map is write
    * @param keyPrefix        prefix to add to the keys, can be omitted
    * @param keyPostfix       postfix to add to the keys, can be omitted
    * @return String containing the mark in the file name
    */
  def mapToFile(map: Map[String, String], writer: Writer, keyPrefix: String = "", keyPostfix: String = ""): Unit = {
    for ((k, v) <- map.toSeq.sortBy(_._1))
      if (!(k.isEmpty || v.isEmpty))  {
        var k_enriched = k.filterNot("\n".toSet).replaceAll("\\s+$", "")
          .replace(" ", "_")
          .replace("%", "perc")
          .replace("+", "plus")
          .replace("/", "or")
          .replaceAll("[^A-Za-z0-9_]", "")
        if (keyPrefix != "")
          k_enriched = keyPrefix + "__" + k_enriched
        if (keyPostfix != "")
          k_enriched = k_enriched + "__" + keyPostfix
        writer.write(s"$k_enriched\t$v\n")
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
  override def getCandidateNames(filename: String, dataset: GMQLDataset, source: GMQLSource): List[String] = {
    List[String](filename.substring(0, filename.lastIndexOf(".")))
  }
}

