package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.Item

class ItemTCGA extends TCGATable with Item {

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId, param)
    case "SIZE" => this.size = insertMethod(this.size.toString, param).toLong
    case "DATE" => this.date = insertMethod(this.date, param)
    case "CHECKSUM" => this.checksum = insertMethod(this.checksum, param)
    case "CONTENTTYPE" => this.contentType = insertMethod(this.contentType, param)
    case "PLATFORM" => insertMethod(this.platform, param) match {
      case "humanmethylation27" => this.platform = "methylation 27K"
      case "humanmethylation450" => this.platform = "methylation 450K"
      case "illumina ga2" => this.platform = "illumina genome analyzer 2"
      case _ => this.platform = insertMethod(this.platform, param)
    }
    case "PIPELINE" => this.pipeline = sortPipeline(insertMethod(this.pipeline, param))
    case "SOURCEURL" => this.sourceUrl = insertMethod(this.sourceUrl, param)
    case "LOCALURL" => this.localUrl = insertMethod(this.localUrl, param)
    case "FILENAME" => this.fileName = insertMethod(this.fileName, param)
    case "SOURCEPAGE" => this.sourcePage = insertMethod(this.sourcePage, param)
    case _ => noMatching(dest)
  }

  def sortPipeline(s: String): String = {
    val pipelineList = s.split(",")
    val pipelineListSorted = pipelineList.sorted
    val sortedString = pipelineListSorted.mkString(", ")
    sortedString
  }

}
