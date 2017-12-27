package it.polimi.genomics.importer.ModelDatabase.TCGA.Table

import it.polimi.genomics.importer.ModelDatabase.Replicate

class ReplicateTCGA extends TCGATable with Replicate {
  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = dest.toUpperCase match {
    case "SOURCEID" => {
      this.sourceId = insertMethod(param, param)
    }
    case "BIOREPLICATENUM" => {
      this.bioReplicateNum= param.toInt
    }
    case "TECHREPLICATENUM" => {
      this.techReplicateNum = param.toInt
    }
    case _ => noMatching(dest)
  }

}