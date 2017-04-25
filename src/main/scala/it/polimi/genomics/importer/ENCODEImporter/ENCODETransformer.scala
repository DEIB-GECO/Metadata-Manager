package it.polimi.genomics.importer.ENCODEImporter

import java.io.{File, _}
import java.util
import java.util.zip.GZIPInputStream

import it.polimi.genomics.importer.GMQLImporter.{GMQLSource, GMQLTransformer, GMQLDataset}
import org.codehaus.jackson.map.MappingJsonFactory
import org.codehaus.jackson.{JsonNode, JsonParser, JsonToken}
import org.slf4j.LoggerFactory

/**
  * Created by Nacho on 10/13/16.
  * Object meant to be used for transform the data from ENCODE to data for GMQL,
  * files must be in the following format:
  *   - metadata file downloaded from ENCODE (1 single file for all the samples)
  *   - .gz data files downloaded from ENCODE.
  */
class ENCODETransformer extends GMQLTransformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  //--------------------------------------------BASE CLASS SECTION------------------------------------------------------


  /**
    * by receiving an original filename returns the new GDM candidate name.
    *
    * @param filename original filename
    * @param dataset dataser where the file belongs to
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset :GMQLDataset, source: GMQLSource): List[String] = {
    val x = 100
    if (filename.endsWith(".gz"))
      List[String](filename.substring(0, filename.lastIndexOf(".")))
    else {
      if (source.parameters.exists(_._1 == "metadata_extraction") &&
        source.parameters.filter(_._1 == "metadata_extraction").head._2 == "json") {
        if (filename.endsWith(".gz.json"))
          List[String](filename.replace(".gz.json", ".meta"))
        else List[String]()
      }

      else if (source.parameters.exists(_._1 == "metadata_extraction") &&
        source.parameters.filter(_._1 == "metadata_extraction").head._2 != "json" &&
        source.parameters.filter(_._1 == "metadata_suffix").head._2.contains(filename)) {
        import scala.io.Source
        val header = Source.fromFile(source.outputFolder +
          File.separator + dataset.outputFolder + File.separator + "Downloads" + File.separator + filename).getLines().next().split("\t")

        //this "File download URL" maybe should be in the parameters of the XML.
        val url = header.lastIndexOf("File download URL")
        Source.fromFile(source.outputFolder +
          File.separator + dataset.outputFolder + File.separator + "Downloads" + File.separator + filename).getLines().drop(1).map(line => {
          //create file .meta
          val fields = line.split("\t")
          val aux1 = fields(url).split("/").last
          val aux2 = aux1.substring(0, aux1.lastIndexOf(".")) + ".meta" //this is the meta name
          aux2
        }).toList
      }
      else List[String]()
    }
  }
  /**
    * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
    * @param source source where the files belong to.
    * @param originPath path for the  "Downloads" folder
    * @param destinationPath path for the "Transformations" folder
    * @param originalFilename name of the original file .json/.gz
    * @param filename name of the new file .meta/.bed
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: GMQLSource,originPath: String, destinationPath: String, originalFilename:String,
                filename: String):Unit= {
      fillMetadataExclusion(source)
      val fileDownloadPath = originPath + File.separator + originalFilename
      val fileTransformationPath = destinationPath + File.separator + filename
      if (originalFilename.endsWith(".gz")) {
        logger.debug("Start unGzipping: " + originalFilename)
        unGzipIt(
          fileDownloadPath,
          fileTransformationPath)
        logger.info("UnGzipping: " + originalFilename + " DONE")
      }
      else if(source.parameters.exists(_._1 == "metadata_extraction") &&
        source.parameters.filter(_._1 == "metadata_extraction").head._2 == "json") {
        if (originalFilename.endsWith(".gz.json")) {
          logger.debug("Start metadata transformation: " + originalFilename)
          val jsonFileName = filename.split('.').head

          val separator =
            if (source.parameters.exists(_._1 == "metadata_name_separation_char"))
              source.parameters.filter(_._1 == "metadata_name_separation_char").head._2
            else
              "|"
          transformMetaFromJson(fileDownloadPath, fileTransformationPath, jsonFileName, separator)
          logger.info("Metadata transformation: " + originalFilename + " DONE")
        }
        else {
          //this is no data nor metadata file, must be the metadata.tsv and is not used due to json selection.
        }
      }
      //if not json is defined, metadata.tsv will be used.
      else {
        if(source.parameters.filter(_._1 == "metadata_suffix").head._2.contains(originalFilename)) {
          transformMetaFromTsv(fileDownloadPath,destinationPath)
        }
        else{
          //is not metadata file
        }
      }
  }

  /**
    * by giving a metadata.tsv file creates all the metadata for the files.
    * @param originPath full path to metadata.tsv file
    * @param destinationFolder transformations folder of the dataset.
    */
  def transformMetaFromTsv(originPath: String, destinationFolder: String): Unit ={
    import scala.io.Source
    logger.info(s"Splitting ENCODE metadata from $originPath")
    val header = Source.fromFile(originPath).getLines().next().split("\t")

    //this "File download URL" maybe should be in the parameters of the XML.
    val url = header.lastIndexOf("File download URL")
    Source.fromFile(originPath).getLines().drop(1).foreach(line => {
      //create file .meta
      val fields = line.split("\t")
      val aux1 = fields(url).split("/").last
      val aux2 = aux1.substring(0, aux1.lastIndexOf(".")) + ".meta" //this is the meta name
      val file = new File(destinationFolder +File.separator+ aux2)

      val writer = new PrintWriter(file)
      for (i <- 0 until fields.size) {
        if (fields(i).nonEmpty)
          writer.write(header(i) + "\t" + fields(i) + "\n")
      }
      writer.close()
    })
  }
  //----------------------------------------METADATA FROM JSON SECTION--------------------------------------------------
  /**
    * by giving a .json file, it generates a .meta file with the json structure.
    * does an exception for the section "files" and needs the file id to achieve this.
    * @param metadataJsonFileName origin json file
    * @param metadataFileName destination .meta file
    * @param fileNameWithoutExtension id of the file being converted.
    */
  def transformMetaFromJson(metadataJsonFileName: String, metadataFileName: String, fileNameWithoutExtension: String
                            ,separator : String): Unit ={
    val jsonFile = new File(metadataJsonFileName)
    if(jsonFile.exists()) {
      val f = new MappingJsonFactory()
      val jp: JsonParser = f.createJsonParser(jsonFile)

      val current: JsonToken = jp.nextToken()
      if (current != JsonToken.START_OBJECT) {
        logger.error("json root should be object: quiting File: " + metadataJsonFileName)
      }
      else {
        val file = new File(metadataFileName)
        val writer = new PrintWriter(file)
        try {
          //this is the one that could throw an exception
          val node: JsonNode = jp.readValueAsTree()

          val metadataList = new java.util.ArrayList[(String,String)]()
          //here I handle the exceptions as "files" and "replicates"
          val replicateIds = getReplicatesAndWriteFile(node, writer, fileNameWithoutExtension, metadataList,separator)
          writeReplicates(node, writer, replicateIds, metadataList,separator)
          //here is the regular case

          printTree(node, "", writer, metadataList,separator, exclusion = true)
        }
        catch {
          case e: IOException => logger.error("couldn't read the json tree: " + e.getMessage)
        }
        writer.close()
      }
    }
    else
      logger.warn("Json file not found: "+metadataJsonFileName)
  }
  /**
    * by getting the exclusion list of encode metadata, generates (if not generated before) the exclusionRegex list.
    * @param source source with the parameters of exclusion for encode.
    */
  def fillMetadataExclusion(source: GMQLSource): Unit ={
    if(exclusionRegex.isEmpty) {
      //fills the exclusion regexes into the list.
      source.parameters.filter(parameter => parameter._1.equalsIgnoreCase("encode_metadata_exclusion"))
        .foreach(param => {
          exclusionRegex.add(param._2)
        })
    }
  }
  /**
    * handles the particular case of files, writes its metadata and returns a list with the replicates IDs used.
    * @param rootNode initial node of the json file.
    * @param writer output for metadata.
    * @param fileNameWithoutExtension id of the file that metadata is being extracted without the .meta.
    * @param metaList list with already inserted meta to avoid duplication.
    * @return list with the replicates referred by the file.
    */
  def getReplicatesAndWriteFile(rootNode: JsonNode, writer: PrintWriter, fileNameWithoutExtension:String,
                                metaList: java.util.ArrayList[(String,String)], separator: String): List[String] ={
    //particular cases first one is to find just the correct file to use its metadata.
    var replicates = List[String]()
    if(rootNode.has("files")){
      val files = rootNode.get("files").getElements
      while (files.hasNext) {
        val file = files.next()
        if (file.has("@id") && file.get("@id").asText().contains(fileNameWithoutExtension)) {
          if(file.has("biological_replicates")){
            val biologicalReplicates = file.get("biological_replicates")
            if(biologicalReplicates.isArray){
              val values = biologicalReplicates.getElements
              while(values.hasNext){
                val replicate = values.next()
                if(replicate.asText() != ""){
                  replicates = replicates :+ replicate.asText()
                }
              }
            }
          }
          //here is where the file is wrote
          printTree(file, "file", writer, metaList,separator, exclusion = false)
        }
      }
    }
    replicates
  }

  /**
    * handles the particular case of biological replicates, writes their metadata from a list of replicates
    * @param rootNode initial node of the json file.
    * @param writer output for metadata.
    * @param replicateIds list with the biological_replicate_number used by the file.
    * @param metaList list with already inserted meta to avoid duplication.
    */
  def writeReplicates(rootNode: JsonNode, writer: PrintWriter, replicateIds:List[String],
                      metaList: java.util.ArrayList[(String,String)], separator: String): Unit ={
    if(rootNode.has("replicates")){
      val replicatesNode = rootNode.get("replicates")
      if(replicatesNode.isArray) {
        val replicates = replicatesNode.getElements
        while (replicates.hasNext){
          val replicate = replicates.next()
          if(replicate.has("biological_replicate_number") &&
            replicateIds.contains(replicate.get("biological_replicate_number").asText()))
            printTree(replicate,s"replicates|${replicate.get("biological_replicate_number").asText()}",writer,metaList,separator, exclusion = false)
        }
      }
    }
  }

  /**
    * gets the "hard coded" exclusion categories, meant to be used for the particular cases
    * Files and Replicates should be always be there, other exclusions are managed from xml file with regex.
    */
  val exclusionCategories: java.util.ArrayList[String] ={
    val list = new java.util.ArrayList[String]()
    list.add("files")
    list.add("replicates")
    list
  }
  /**
    * loads from the xml "encodeMetadataConfig" the regex set to be excluded fromt he metadata.
    */
  var exclusionRegex: java.util.ArrayList[String] = new util.ArrayList[String]()


  /**
    * by giving an initial node, prints into the .meta file its metadata and its children's metadata also.
    * I use java arraylist as scala list cannot be put as var in the parameters.
    * @param node current node
    * @param parents path separated by dots for each level
    * @param writer file writer with the open .meta file
    * @param metaList list with already inserted meta to avoid duplication.
    */
  def printTree(node: JsonNode, parents: String, writer: PrintWriter, metaList: java.util.ArrayList[(String,String)], separator: String, exclusion: Boolean): Unit = {
    //base case, the node is value
    if(node.isValueNode && node.asText() != ""
    && !metaList.contains(node.asText(),parents)
    ) {
      writer.write(parents + "\t" + node.asText() + "\n")
      metaList.add((node.asText(),parents))
    }
    else {
      val fields: util.Iterator[String] = node.getFieldNames
      while (fields.hasNext) {
        val name = fields.next()
        if (!exclusionCategories.contains(name) || !exclusion) {
          val element = node.get(name)
          //base case when parents are empty
          val currentName = if (parents == "") name else parents + separator + name
          //check the regex
          var regexMatch = false
          for(i<-0 until exclusionRegex.size())
            if(currentName.matches(exclusionRegex.get(i)))
              regexMatch = true
          if(!regexMatch) {
            if (element.isArray) {
              val subElements = element.getElements
              while (subElements.hasNext)
                printTree(subElements.next(), currentName, writer, metaList, separator, exclusion)
            }
            else
              printTree(element, currentName, writer, metaList, separator, exclusion)
          }
        }
      }
    }
  }

//  //--------------------------------------------SCHEMA SECTION----------------------------------------------------------
//  /**
//    * using information in the loader should arrange the files into a single folder
//    * where data and metadata are paired as (file,file.meta) and should put also
//    * the schema file inside the folder.
//    * ENCODE schema file is not provided in the same folder as the data
//    * for the moment the schemas have to be given locally.
//    *
//    * @param source contains specific download and sorting info.
//    */
//  def organize(source: GMQLSource): Unit = {
//    source.datasets.foreach(dataset => {
//      if(dataset.transformEnabled) {
//        if (dataset.schemaLocation == SCHEMA_LOCATION.LOCAL) {
//          val src = new File(dataset.schemaUrl)
//          val dest = new File(source.outputFolder + File.separator + dataset.outputFolder + File.separator +
//            "Transformations" + File.separator + dataset.name + ".schema")
//
//          try {
//            Files.copy(src, dest)
//            logger.info("Schema copied from: " + src.getAbsolutePath + " to " + dest.getAbsolutePath)
//          }
//          catch {
//            case e: IOException => logger.error("could not copy the file " +
//              src.getAbsolutePath + " to " + dest.getAbsolutePath)
//          }
//        }
//      }
//    })
//  }
  //-------------------------------------UTILS -------------------------------------------------------------------------
  /**
    * extracts the gzipFile into the outputPath.
    *
    * @param gzipFile   full location of the gzip
    * @param outputPath full path of destination, filename included.
    */
  def unGzipIt(gzipFile: String, outputPath: String): Unit = {
    val bufferSize = 1024
    val buffer = new Array[Byte](bufferSize)

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
    } catch {
      case e: IOException => logger.error("Couldnt UnGzip the file: " + outputPath, e)
    }
  }
}
