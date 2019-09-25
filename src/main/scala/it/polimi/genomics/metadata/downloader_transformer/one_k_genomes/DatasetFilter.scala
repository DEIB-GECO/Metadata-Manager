package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.regex.Pattern

import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}    // converts java.util.List into scala.util.List in order to use foreach


/**
 * Created by Tom on set, 2019
 *
 * Helper class capable of reading large files of ASCII characters and filtering each line against a regular expression
 */
object DatasetFilter {

  class DatasetPattern {
    val ANY_CHAR_SEQUENCE = "\\S+"
    val POSSIBLY_EMPTY_CHAR_SEQ = "\\S*"
    val ANYTHING = ".*"
    val BLANK = "\\s+"
    val FILE = "file"
    val DIRECTORY = "directory"
    val NO_SUBDIR = "[^/]*"

    private var path: String = ANY_CHAR_SEQUENCE
    private var fileType: String = ANY_CHAR_SEQUENCE
    private var size = ANY_CHAR_SEQUENCE
    private var dayOfWeek: String = ANY_CHAR_SEQUENCE
    private var month: String = ANY_CHAR_SEQUENCE
    private var dayOfMonth: String = ANY_CHAR_SEQUENCE
    private var time: String = ANY_CHAR_SEQUENCE
    private var year: String = ANY_CHAR_SEQUENCE
    private var md5: String = ANY_CHAR_SEQUENCE

    private var excludeSubDir = false
    private var customPathRegex = false
    private var pathEnding = ""

    def filterPathBeginWith(path: String): DatasetPattern = {
      this.path = path
      this
    }

    def filterPathInsideDir(dirPath: String): DatasetPattern = {
      this.path = dirPath.endsWith("/") match {
        case true => dirPath
        case false => dirPath+"/"
      }
      this
    }

    def filterPathWithRegex(regex: String): DatasetPattern = {
      customPathRegex = true
      this.path = regex
      this
    }

    def filterPathGRCh38(): DatasetPattern = {
//      TODO get this from configuration file
      path = "ftp/data_collections/1000_genomes_project/release/"
      this
    }

    def filterPathExcludeSubdirs(): DatasetPattern = {
      excludeSubDir = true
      this
    }

    def filterPathEndsWith(ending: String): DatasetPattern ={
      pathEnding = ending
      this
    }

    def filterFiles(): DatasetPattern = {
      fileType = FILE
      this
    }

    def filterDirectories(): DatasetPattern = {
      fileType = DIRECTORY
      this
    }

    def get(): Pattern = {
      PatternMatch.createPattern(getRegex())
    }

    def getRegex(): String = {
      val _path = if (customPathRegex) path
      else {
        if (excludeSubDir)
          "(" + path + NO_SUBDIR + pathEnding + ")"
        else
          "(" + path + POSSIBLY_EMPTY_CHAR_SEQ + pathEnding + ")"
      }
      val middle =
        _path + BLANK +
          "(" + fileType + ")" + BLANK +
          "(" + size + ")" + BLANK +
          "(" + dayOfWeek + ")" + BLANK +
          "(" + month + ")" + BLANK +
          "(" + dayOfMonth + ")" + BLANK +
          "(" + time + ")" + BLANK +
          "(" + year + ")"
      val ending = fileType match {
        case DIRECTORY => ""
        case FILE => BLANK + md5
        case _ => "(\\s+(\\S+))?" // optional md5 hash
      }
      middle+ending
    }

  }

  def dirPathLatestVariantsGRCH38(treeLocalPath: String): String ={
    // records related to GRCh38 directories and subdirectories
    val datasetPattern = (new DatasetPattern).filterPathGRCh38().filterPathExcludeSubdirs().filterDirectories().get()
    val fileReader = FileUtil.open(treeLocalPath).get
    val datasetDirsRecords = PatternMatch.getLinesMatching(datasetPattern, fileReader)
    fileReader.close()
    // debug
    println("FILTERED LINES:")
    datasetDirsRecords.foreach {println}
    // list of direct subdirectories sorted by name
    val sortedSubDirectories = (for {
      record <- datasetDirsRecords
      partsOfRecord = PatternMatch.matchParts(record, (new DatasetPattern).get())
      if partsOfRecord.nonEmpty
    } yield partsOfRecord.head).sorted
    println("SUBDIRS PATH:")
    sortedSubDirectories.foreach {println}
    // choose the latest one
    println(s"LATEST ONE IS :${sortedSubDirectories.last}")
    sortedSubDirectories.last
  }



  def latestVariantsFromDir(directoryPath: String, treeLocalPath: String): List[String] = {
    // records from directoryPath
    val variantsPattern = (new DatasetPattern).filterPathInsideDir(directoryPath)
      .filterPathEndsWith(".vcf.gz").filterPathExcludeSubdirs().filterFiles().get()
    val fileReader = FileUtil.open(treeLocalPath).get
    val variantRecords = PatternMatch.getLinesMatching(variantsPattern, fileReader)
    fileReader.close()
//    debug
//    writeFile("C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager-WorkDir\\MD\\filtered.tree.tsv", variantRecords)
    variantRecords
  }

  /**
   * @param dataset a xml.Dataset having parameters "sequence_index_file_path" and "population_file_path" (the latter
   *                can also be at source level)
   * @return the paths of the metadata files relative to the given dataset
   */
  def metadataPaths(dataset : Dataset): List[String] ={
    List(
      dataset.getDatasetParameter("sequence_index_file_path").get,
      dataset.getParameter("population_file_path").get
    )
  }

//  DEBUG
  def writeFile(where: String, content: List[String]): Unit ={
    // create & replace
    val writer = Files.newBufferedWriter(Paths.get(where), StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    content.foreach(line => {
      writer.write(line)
      writer.newLine()
    })
    writer.close()
  }
}
