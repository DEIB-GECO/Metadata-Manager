package it.polimi.genomics.importer.DefaultImporter

import java.io.{File, IOException}

import it.polimi.genomics.importer.GMQLImporter.{GMQLSource, GMQLTransformer, GMQLDataset}
import org.slf4j.LoggerFactory

/**
  * Created by Nacho on 10/13/16.
  */
class NULLTransformer extends GMQLTransformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * by receiving an original filename returns the new GDM candidate name.
    *
    * @param filename original filename
    * @param dataset dataser where the file belongs to
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset :GMQLDataset, source: GMQLSource): List[String] = {
    var returns = true
    val filenameSplit = filename.split('.')
    if(filenameSplit.nonEmpty){
      val name = filenameSplit.head
      val filenameSplitDrop = filenameSplit.drop(1)
      if(filenameSplitDrop.nonEmpty) {
        val extension = filenameSplitDrop.head
        if (dataset.parameters.exists(_._1 == "md5_checksum_tcga2bed")) {
          val head = dataset.parameters.filter(_._1 == "md5_checksum_tcga2bed").head
          val headSplit = head._2.split('.')
          if (headSplit.nonEmpty) {
            val compareName = headSplit.head
            val headSplitDrop = headSplit.drop(1)
            if(headSplitDrop.nonEmpty){
              val compareExtension = headSplitDrop.head
              if(name.contains(compareName) && extension.contains(compareExtension))
                returns = false
            }
          }
        }
        if (dataset.parameters.exists(_._1 == "exp_info_tcga2bed")) {
          val head = dataset.parameters.filter(_._1 == "exp_info_tcga2bed").head
          val headSplit = head._2.split('.')
          if (headSplit.nonEmpty) {
            val compareName = headSplit.head
            val headSplitDrop = headSplit.drop(1)
            if(headSplitDrop.nonEmpty){
              val compareExtension = headSplitDrop.head
              if(name.contains(compareName) && extension.contains(compareExtension))
                returns = false
            }
          }
        }
      }
    }
    if(returns)
      List[String](filename)
    else
      List[String]()
  }

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
  override def transform(source: GMQLSource, originPath: String, destinationPath: String, originalFilename: String,
                filename: String): Boolean = {
    val fileTransformationPath = destinationPath + File.separator + filename
    val fileDownloadPath = originPath + File.separator + originalFilename
    var timesTried = 0
    var transformed = false
    while(timesTried < 4 && !transformed)
    try {
      import java.io.{File, FileInputStream, FileOutputStream}
      val src = new File(fileDownloadPath)
      val dest = new File(fileTransformationPath)
      new FileOutputStream(dest) getChannel() transferFrom(
        new FileInputStream(src) getChannel(), 0, Long.MaxValue)
      //here have to add the metadata of copy number and total copies
      logger.info("File: " + fileDownloadPath + " copied into " + fileTransformationPath)
      transformed = true;
    }
    catch {
      case e: IOException => timesTried +=1
    }
    if(!transformed)
      logger.warn("could not copy the file " +
      fileDownloadPath + " to " +
      fileTransformationPath)
    transformed
  }

}