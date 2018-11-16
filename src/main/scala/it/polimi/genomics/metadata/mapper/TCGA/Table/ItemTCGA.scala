package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.Item

class ItemTCGA extends TCGATable with Item{

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SIZE" => this.size = insertMethod(this.size.toString,param).toLong
    case "DATE" => this.date = insertMethod(this.size.toString,param)
    case "CHECKSUM" => this.checksum = insertMethod(this.size.toString,param)
    case "PLATFORM" => this.platform = sortPipeline(insertMethod(this.platform, param))
    case "PIPELINE" => this.pipeline = insertMethod(this.pipeline,param)
    case "SOURCEURL" => this.sourceUrl = insertMethod(this.sourceUrl,param)
    case "LOCALURL" => this.localUrl = insertMethod(this.localUrl,param)
    case _ => noMatching(dest)
  }

  def sortPipeline(s: String): String={
    val pipelineList = s.split(",")
    val pipelineListSorted = pipelineList.sorted
    val sortedString = pipelineListSorted.mkString(", ")
    sortedString
  }

}
