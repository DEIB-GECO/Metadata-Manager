package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.regex.Pattern

import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}
import org.slf4j.{Logger, LoggerFactory}    // converts java.util.List into scala.util.List in order to use foreach


/**
 * Created by Tom on set, 2019
 *
 * Helper class capable of reading large files of ASCII characters and filtering each line against a regular expression
 */
object DatasetInfo {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * This class helps in the generation of regular expressions and java.util.regex.Pattern objects matching the
   * records present in the tree file present at 1kGenomes FTP server. The generated regex or patterns can
   * then be used for filtering the tree records and parse its content. */
  class DatasetPattern {
    val ANY_CHAR_SEQUENCE = "\\S+"
    val POSSIBLY_EMPTY_CHAR_SEQ = "\\S*"
    val ANYTHING = ".*"
    val BLANK = "\\s+"
    val FILE = "file"
    val DIRECTORY = "directory"
    val NO_SUBDIR = "[^/]*"

    private var path: String = ""
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
    private var insideDir: String = ""
    private var pathEnding = ""
    private var _splitTimestamp = false

    /**
     * Generic method to describe how the file path should begin without distinction between files or folders.
     * This method is not designed to get regular expressions as argument.
     *
     * @throws IllegalArgumentException if curly braces appear in the argument. Curly braces are used in regex to
     *                                  specify regex groups. If you need this capability use instead @filterPathWithRegex
     */
    def filterPathBeginWith(path: String): DatasetPattern = {
      if(path.contains("(") || path.contains(")") || path.contains(".*"))
        throw new IllegalArgumentException("CURLY BRACES ARE RESERVED CHARACTERS. " +
          "INSTEAD USE filterPathWithRegex IF YOU NEED TO SPECIFY GROUPS INSIDE THE FILE PATH")
      this.path = path
      this
    }

    /**
     * Similar to @filterPathBeginWith but the resulting regex will match only the content ***inside*** the folder given as
     * argument while the folder itself is excluded.
     * This method is not designed to get regular expressions as argument.
     *
     * @throws IllegalArgumentException if curly braces appear in the argument. Curly braces are used in regex to
     *                                  specify regex groups. If you need this capability use instead @filterPathWithRegex
     * @param dirPath path of a directory whose content must be included in the files extracted from the regex.
     */
    def filterPathInsideDir(dirPath: String): DatasetPattern = {
      if(dirPath.contains("(") || dirPath.contains(")") || dirPath.contains(".*"))
        throw new IllegalArgumentException("CURLY BRACES ARE RESERVED CHARACTERS. " +
          "INSTEAD USE filterPathWithRegex IF YOU NEED TO SPECIFY GROUPS INSIDE THE FILE PATH")
      this.insideDir = dirPath.endsWith("/") match {
        case true => dirPath
        case false => dirPath+"/"
      }
      this
    }

    /**
     * This gives full control over the file path regex and allows to specify any number of grouping over the file path
     * or to specify none.
     *
     * @param regex a regular expression String compatible with java regular expressions
     */
    def filterPathWithRegex(regex: String): DatasetPattern = {
      customPathRegex = true
      this.path = regex
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
    
    def splitTimestamp(): DatasetPattern = {
      _splitTimestamp = true
      this
    }

    def get(): Pattern = {
      PatternMatch.createPattern(getRegex)
    }

    def getRegex: String = {
      val _path = if (customPathRegex) path
      else {
        "(" +
        insideDir +
        {if(excludeSubDir) NO_SUBDIR else POSSIBLY_EMPTY_CHAR_SEQ} +
        path +
        {if(excludeSubDir) NO_SUBDIR else POSSIBLY_EMPTY_CHAR_SEQ} +
        pathEnding +
        ")"
      }
      val middle_1 =
        BLANK +
        "(" + fileType + ")" + BLANK +
        "(" + size + ")" + BLANK
      val timestamp = if (_splitTimestamp) {
        "(" + dayOfWeek + ")" + BLANK +
        "(" + month + ")" + BLANK +
        "(" + dayOfMonth + ")" + BLANK +
        "(" + time + ")" + BLANK +
        "(" + year + ")"
      } else {
        "("+ dayOfWeek +  BLANK +
          month +  BLANK +
          dayOfMonth +  BLANK +
          time +  BLANK +
          year + ")"
      }
      val ending = fileType match {
        case DIRECTORY => ""
        case FILE => BLANK + md5
        case _ => "(\\s+(\\S+))?" // optional md5 hash
      }
      _path + middle_1 + timestamp + ending
    }

  }

  def latestVariantsRecords(treeFilePath: String, dataset: Dataset): List[String] = {
    /**
     * Given the base location of a dataset and a tree file, parses the names of the direct sub-directories to determine
     * which contains the latest version of the dataset and returns its path.
     *
     * The method takes advantage of the knowledge on how data sets are organized inside 1kGenomes FTP server:
     * old and new releases of the same data set are never overwritten, instead they're organized in sub-folders starting
     * from a base directory location. Data sets for assemblies GRCh38 and hg19 are located at different base directory paths.
     */
    def latestVariantSubdirectory(baseDir: String, treeLocalPath: String): String ={
      val datasetPattern = (new DatasetPattern).filterPathInsideDir(baseDir).filterPathExcludeSubdirs().filterDirectories().get()
      val fileReader = FileUtil.open(treeLocalPath).get
      val datasetDirsRecords = PatternMatch.getLinesMatching(datasetPattern, fileReader)
      fileReader.close()
      // debug
      logger.info("FILTERED LINES:")
      datasetDirsRecords.foreach {logger.info}
      // list of direct subdirectories sorted by name
      val sortedSubDirectories = (for {
        record <- datasetDirsRecords
        partsOfRecord = PatternMatch.matchParts(record, (new DatasetPattern).get())
        if partsOfRecord.nonEmpty
      } yield partsOfRecord.head).sorted
      logger.info("SUBDIRS PATH:")
      sortedSubDirectories.foreach {logger.info}
      // choose the latest one
      logger.info(s"LATEST ONE IS :${sortedSubDirectories.last}")
      sortedSubDirectories.last
    }

    /**
     * Reads the tree file and identifies the records of the variants inside a given directory. The behaviour of this
     * method is controllable through the following XML config parameters:
     * filter_variants_starting_characters
     * filter_variants_ending_characters
     * exclude_subdirs_in_each_dataset_release
     * filter_variants_with_custom_path_regex
     *
     * @param directoryPath limits the search to the files contained within the given directory path
     *                      (without the URL prefix described in url_prefix_tree_file_records)
     * @param treeLocalPath path to the local tree file
     * @param dataset of the variants to search for. It's used to customize the filter parameters.
     * @return a list of String, each one corresponding to a record in the tree file.
     */
    def variantsFromDir(directoryPath: String, treeLocalPath: String, dataset: Dataset): List[String] = {
      // get optional XML params to filter the variants of interest in a dataset (remote) directory
      val startingChars = dataset.getParameter("filter_variants_starting_characters")
      val endingChars = dataset.getParameter("filter_variants_ending_characters")
      val excludeSubdirs = dataset.getParameter("exclude_subdirs_in_each_dataset_release").map(_.toBoolean)
      val customPathRegex = dataset.getParameter("filter_variants_with_custom_path_regex")
      // build pattern for the records
      val variantsPattern = if(customPathRegex.isDefined)
        (new DatasetPattern).filterPathWithRegex(customPathRegex.get).filterFiles()
      else
        (new DatasetPattern).filterPathInsideDir(directoryPath)
      if(startingChars.isDefined) variantsPattern.filterPathBeginWith(startingChars.get)
      if(endingChars.isDefined) variantsPattern.filterPathEndsWith(endingChars.get)
      if(excludeSubdirs.getOrElse(false)) variantsPattern.filterPathExcludeSubdirs()
      variantsPattern.filterFiles()
      val fileReader = FileUtil.open(treeLocalPath).get
      val variantRecords = PatternMatch.getLinesMatching(variantsPattern.get(), fileReader)
      fileReader.close()
      //    debug
      //    writeFile("C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager-WorkDir\\MD\\filtered.tree.tsv", variantRecords)
      variantRecords
    }

    val baseRemoteDatasetDir = dataset.getParameter("dataset_remote_base_directory").getOrElse(
      throw new NullPointerException("MISSING REQUIRED PARAMETER dataset_remote_base_directory AT DATASET LEVEL IN XML CONFIG FILE"))
    // select the subdir containing the latest version of this dataset
    val latestDatasetDirPath = latestVariantSubdirectory(baseRemoteDatasetDir, treeFilePath)
    // select the variant files within
    variantsFromDir(latestDatasetDirPath, treeFilePath, dataset)
  }

  /**
   * @param dataset a xml.Dataset having parameters "sequence_index_file_path" and "population_file_path" (the latter
   *                can also be at source level)
   * @return a List of Strings containing the value of those XML parameter.
   */
  def metadataPaths(dataset : Dataset): List[String] ={
    List(
      dataset.getDatasetParameter("sequence_index_file_path"),
      dataset.getParameter("population_file_path")
    ).filter(_.isDefined).map(option => option.get)
  }

  def metadataRecords(treeLocalPath: String, dataset: Dataset): List[String] = {
    val metaPaths = metadataPaths(dataset)
    val metaPatterns = metaPaths.map(singleMeta => {
      // search and extract the matching record (exact match)
      val metadataPattern = (new DatasetPattern).filterPathWithRegex(singleMeta).filterFiles()
//      logger.debug("SEARCHING MATCHING METADATA RECORDS FOR "+singleMeta)
//      logger.debug("WITH REGEX "+metadataPattern.getRegex())
      metadataPattern.get()
    })
    metaPatterns.flatMap(pattern => {
      val fileReader = FileUtil.open(treeLocalPath).get
      val recordsOfMeta = PatternMatch.getLinesMatching(pattern, fileReader)
      fileReader.close()
      recordsOfMeta
    })
  }

  /**
   * This method simply returns the substring obtained by cutting the original string right after the last slash occurrence
   * untill the end of the string.
   */
  def parseFilenameFromURL(filePath: String): String ={
    if(filePath.endsWith("/"))
      throw new IllegalArgumentException("YOU'RE PROBABLY USING THIS METHOD IMPROPERLY BY PASSING A DIRECTORY PATH AS ARGUMENT")
    filePath.substring(filePath.lastIndexOf("/")+1)
  }

  def getDownloadDir(dataset: Dataset): String ={
    s"${dataset.source.outputFolder}/${dataset.outputFolder}/Downloads/"
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
