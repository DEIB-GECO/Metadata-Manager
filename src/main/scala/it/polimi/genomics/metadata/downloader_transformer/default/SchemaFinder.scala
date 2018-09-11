package it.polimi.genomics.metadata.downloader_transformer.default
import java.io.{File, FileWriter}

import it.polimi.genomics.metadata.step.utils.SchemaLocation
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}

/**
  * Created by Nacho on 12/13/16.
  */
object SchemaFinder {
  /**
    * Puts the dataset schema inside the given folder (meant to be the transformations one).
    * schema is given the dataset name as name and .schema as extension.
    * @param rootFolder root working directory.
    * @param dataset GMQL dataset.
    * @param outputFolder folder where to put the schema.
    * @param source gmql source
    */
  def downloadSchema(rootFolder: String, dataset: Dataset, outputFolder: String, source: Source): Boolean ={
    val outputPath = outputFolder+File.separator+dataset.name+".schema"
    if(!new File(outputFolder).exists())
      new File(outputFolder).mkdirs()
    try {
      if(!new File(outputPath).exists()){
        val writer = new FileWriter(outputPath, true)
        writer.close()
      }
    }
    catch {
      case e: Exception =>false
    }
    dataset.schemaLocation match{
      case SchemaLocation.HTTP=>
        val downloader = new HttpDownloader()
        if(downloader.urlExists(dataset.schemaUrl)) {
          downloader.downloadFileFromURL(dataset.schemaUrl, outputPath)
          true
        }
        else false
      case SchemaLocation.LOCAL =>
        if(new File(rootFolder+File.separator+dataset.schemaUrl).exists()) {
          import java.io.{File, FileInputStream, FileOutputStream}
          val src = new File(rootFolder + File.separator + dataset.schemaUrl)
          val dest = new File(outputPath)
          new FileOutputStream(dest) getChannel() transferFrom(
            new FileInputStream(src).getChannel, 0, Long.MaxValue)
          true
        }
        else false
      case SchemaLocation.FTP =>
        val downloader = new FtpDownloader()
        val workingDirectory = dataset.schemaUrl.substring(0,dataset.schemaUrl.lastIndexOf(File.separator))
        val filename = dataset.schemaUrl.split(File.separator).last
        if(downloader.downloadFile(outputPath,source, workingDirectory,filename,"",1,1))
          true
        else
          false

    }
  }
}
