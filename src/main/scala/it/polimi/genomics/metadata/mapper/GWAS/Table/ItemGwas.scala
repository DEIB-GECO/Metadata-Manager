package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.Item
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId

class ItemGwas(gwasTableId: GwasTableId) extends GwasTable(gwasTableId)  with Item {
  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SIZE" => this.size = insertMethod(this.size.toString,param).toLong
    case "DATE" => this.date = insertMethod(this.date,param)
    case "CHECKSUM" => this.checksum = insertMethod(this.checksum,param)
    case "PLATFORM" => this.platform = insertMethod(this.platform, param)
    case "SOURCEURL" => this.sourceUrl = insertMethod(this.sourceUrl,param)
    case "SOURCEPAGE" => this.sourcePage = insertMethod(this.sourcePage,param)
    case "LOCALURL" => this.localUrl = insertMethod(this.localUrl,param)
    case "FILENAME" => this.fileName = insertMethod(this.fileName,param)
    case _ => noMatching(dest)
  }
}
