package it.polimi.genomics.metadata.downloader_transformer.gwas

import java.io.{File, _}
import it.polimi.genomics.metadata.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.FtpDownloader
import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.step.xml
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import scala.util.matching.Regex

class GwasTransformer extends Transformer {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val patternMetadata: Regex = """.*\.meta""".r
  val patternRegionData: Regex = """GCST[0-9]+\.gdm""".r

  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    * The names are extracted from the associations file.
    *
    * @param filename original filename
    * @param dataset  dataset where the file belongs to
    * @param source   source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset: Dataset, source: xml.Source): List[String] = {
    val accessionList = new ListBuffer[String]
    val candidates = new ListBuffer[String]
    if (filename.contains("associations")){
      val inFolder = source.outputFolder + File.separator + "latest" + File.separator + "Downloads"
      //val inFolder = source.outputFolder + "\\latest\\Downloads"
      val reader = Source.fromFile(inFolder + File.separator + filename)
      //val reader = Source.fromFile(inFolder + "\\" + filename)
      reader.getLines().drop(1).foreach(line => {
      //reader.getLines().drop(1).take(20).foreach(line => {
        val tmp = line.split("\t")
        val flag = (tmp.length == 38 && (tmp(20).contains("chr") || tmp(20).contains("Chr")))
        if (tmp.length == 38 && (tmp(11)!="" || flag)){ accessionList += tmp(36)}
      })
      reader.close()
      //accessionList.distinct.take(500).foreach(acc => {
      accessionList.distinct.foreach(acc => {
        candidates += acc + ".gdm"
        candidates += acc + ".gdm.meta"
      })
      candidates.toList
    } else {
      candidates.toList
    }
  }

  /**
    * receives candidates "GCSTXXXXXX.gdm" and "GCSTXXXXXX.gdm.meta" and creates the corresponding
    * regions and meta files.
    *
    * @param source           source where the files belong to
    * @param originPath       path for the  "Downloads" folder
    * @param destinationPath  path for the "Transformations" folder
    * @param originalFilename name of the original file
    * @param filename         name of the new file
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: xml.Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    var isTransformationDone: Boolean = true //false if an error occurs during the transformation
    filename match {
      case patternMetadata() =>
        val metaGenOutcome = Try(metaGen(filename, originPath, destinationPath))
        if (metaGenOutcome.isSuccess) {
          logger.info("metaGen: " + filename + " DONE")
        }
        else {
          isTransformationDone = false
          logger.warn("metaGen: " + filename + " FAILED", metaGenOutcome.failed.get)
        }
      case patternRegionData() =>
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
  * generates the file "GCSTXXXXXX.gdm" in the Transformations folder with the region data extracted from the
  * gwas-catalog-associations_ontology-annotated.tsv.
  *
  * @param source            source where the files belong to
  * @param filename          name of the file to generate
  * @param originPath        path for the  "Downloads" folder
  * @param destinationPath   path for the "Transformations" folder
  */
  def regionTransformation(source: xml.Source, filename: String, originPath: String, destinationPath: String): Unit = {
    val readerRegion = Source.fromFile(originPath + File.separator + "gwas-catalog-associations_ontology-annotated.tsv" )
    val transformedFile = new File(destinationPath + File.separator + filename)
    val writer = new PrintWriter(transformedFile.getAbsolutePath)
    val acc = filename.substring(0, filename.lastIndexOf("."))
    readerRegion.getLines().drop(1).foreach(line => {
      val flag = line.split("\t").length == 38 && (line.split("\t")(20).contains("chr") || line.split("\t")(20).contains("Chr"))
      val current = (line.contains(acc) && (line.split("\t")(11)!="" || flag))
      if (current) {
        var regionLine = new ListBuffer[String]()
        var count = 10
        //chromosome in which is the SNP
        var chr = "0"
        if(line.split("\t")(11)=="" && flag){

          val tmp = line.split("\t")(20)
          var start = 0
          if(tmp.contains("chr")){
            start = tmp.lastIndexOf("chr")
          } else start = tmp.lastIndexOf("Chr")
          start = start + 3
          // Chr:1:54011503-?
          if (!tmp.charAt(start).isDigit) start = start + 1

          // hg18_chr11:27369242-A
          // chr13_73243177-D-?
          // Chr3:29902302-C
          if (tmp.charAt(start + 1).isDigit) chr = tmp.substring(start, start+2)
          else chr = tmp.charAt(start).toString

        } else chr = line.split("\t")(count + 1)
        val chrom = "chr" + chr
        regionLine += chrom
        //starting position of the SNP in the chromosome
        val start = line.split("\t")(count + 2)
        regionLine += start
        //last position of the SNP in the chromosome
        val tmp = line.split("\t")(count + 2)
        //check if the chrom position is a number
        val tmp2 = tmp.toCharArray
        var bool = true
        if(tmp2.length == 0) bool = false
        for(i <- tmp2){if (!i.isDigit) bool=false}
        var stop = ""
        if(bool) stop = (line.split("\t")(count + 2).toLong + 1).toString
          else stop = tmp
        regionLine += stop
        //strand
        regionLine += "*"
        //region
        regionLine += line.split("\t")(count)
        count += 3
        //other attributes
        while (count < 32){
          //STRONGEST SNP-RISK ALLELE
          if (count == 20) {
            var all = line.split("\t")(count).split("-")(1)
            regionLine += all
            count += 1
          }
          else {
            regionLine += line.split("\t")(count)
            count += 1
          }

        }
        var index=1
        regionLine.foreach(d => {
          if(index != regionLine.size) writer.write(d+"\t")
          else writer.write(d)
          index += 1
        })
        writer.write("\n")
      }
    })
    writer.close()
    readerRegion.close()

  }

  /**
    * generates a "GCSTXXXXXX.gdm.meta" file containing metadata attributes name and value associated to an input file.
    * Each row contains a couple (name \t value).
    *
    * @param inPath   path for the  "Downloads" folder
    * @param outPath  path for the "Transformations" folder
    * @param fileName name of the input file for which the .meta file must be generated
    * @return boolean asserting if the meta file is correctly generated
    */
  def metaGen(fileName: String, inPath: String, outPath: String): Unit = {
    val ancestryReader = Source.fromFile(inPath + File.separator + "gwas-catalog-ancestry.tsv")
    val studiesReader = Source.fromFile(inPath + File.separator + "gwas-catalog-studies_ontology-annotated.tsv")
    //val accession = fileName.substring(0, 10) IT'S WRONG!
    val accession = fileName.substring(0, fileName.lastIndexOf(".")-4)
    val writer = new PrintWriter(outPath + File.separator + fileName)

    //extraction of metadata from studies file
    var studiesHeader = new ListBuffer[String]()
    studiesReader.getLines().take(1).foreach(line =>{
      val params = line.split("\t")
      params.foreach(p => {studiesHeader += p})
    })

    var studiesValues = new ListBuffer[String]()
    val studiesReader2 = Source.fromFile(inPath + File.separator + "gwas-catalog-studies_ontology-annotated.tsv")
    studiesReader2.getLines().drop(1).foreach(line => {
      val current = line.contains(accession)
      if(current) {
        val values = line.split("\t")
        values.foreach(v => {
          //some cohorts don't have the traitName field (it's empty)
          var t = ""
          if(v.equals("")) {t = "NR"} else {t = v}
          studiesValues += t
        })
      }
    })
    val studiesMeta = studiesHeader.zip(studiesValues)

    //Here I could make a check on the empty values
    for ( (k,v) <- studiesMeta) {
      //added 24_09_2021 by Federico Comolli
      if ((!v.isEmpty) && (k.contains("name") || (k.contains("TRAIT")))) { //check if traitName has quotation marks
        val t = v.replaceAll("\"", "")
        writer.write(s"$k\t$t\n")
      } else if (!v.isEmpty) writer.write(s"$k\t$v\n")
    }

    //extraction of metadata from ancestry file
    var ancestryValues = new ListBuffer[String]()
    var numOfRows = 0
    ancestryReader.getLines().drop(1).foreach(line => {
      val current = line.contains(accession)
      if(current) {
        numOfRows += 1
        //values contains from STAGE to ADDITIONAL ANCESTRY DESCRIPTION
        var values = line.split("\t").drop(6)
        if(values.length == 5) values :+= ""
        values.foreach(v => {
            //some cohorts don't have the numberOfIndividual field (it's empty)
            var t = ""
            if(v.equals("")) {t = "NR"} else {t = v}
            ancestryValues += t
        })
      }
    })

    var ancestryHeader = new ListBuffer[String]()
    val ancestryReader2 = Source.fromFile(inPath + File.separator + "gwas-catalog-ancestry.tsv")
    ancestryReader2.getLines().take(1).foreach(line =>{
      val params = line.split("\t").drop(6)
      var count = 1
      while(numOfRows > 0){
        params.foreach(p => {
          if (p.contains("NUMBER OF INDIVDUALS")) {ancestryHeader += "NUMBER OF INDIVIDUALS" + "_" + count}
            else {ancestryHeader += p+ "_" + count}
        })
        count += 1
        numOfRows -= 1
      }
    })
    val ancestryMeta = ancestryHeader.zip(ancestryValues)

    //Here I could make a check on the empty values
    for ( (k,v) <- ancestryMeta) {
      if (!v.isEmpty) writer.write(s"$k\t$v\n")
    }
    writer.close()
  }
}
