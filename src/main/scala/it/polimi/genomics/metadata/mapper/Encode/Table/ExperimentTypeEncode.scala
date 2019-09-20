package it.polimi.genomics.metadata.mapper.Encode.Table

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.ExperimentType

class ExperimentTypeEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with ExperimentType {

  var ontologicalCode: String = _

  var originalKey: String = _

  //var originalValue: String = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase()  match{
    case "TECHNIQUE" => this.technique = insertMethod(this.technique,param)
    case "FEATURE" => this.feature = insertMethod(this.feature,param)
    case "TARGET" => this.target = insertMethod(this.target,param)
    case "ANTIBODY" => this.antibody = insertMethod(this.antibody,param)
    case "ONTOLOGICALCODE" => this.ontologicalCode = insertMethod(this.ontologicalCode,param)
    case "ORIGINALKEY" => this.originalKey = insertMethod(this.originalKey, param)
    case _ => noMatching(dest)

  }

  override def insert(): Int = {
    val id: Int = super.insert()
   // if(conf.getBoolean("import.support_table_insert"))
    //  insertOrUpdateOntologicTuple(id)
    id
  }

  override def updateById(): Unit = {
    super.updateById()
  //  if(conf.getBoolean("import.support_table_insert"))
   //   insertOrUpdateOntologicTuple(this.primaryKey)
  }

/* def insertOrUpdateOntologicTuple(id: Int): Unit = {
   if(dbHandler.checkInsertOntology(id, "experiment_type", "technique"))
     dbHandler.insertOntology(id, "experiment_type", "technique", this.ontologicalCode.split('*')(0), this.technique, this.ontologicalCode.split('*')(1))
   else
     dbHandler.updateOntology(id, "experiment_type", "technique", this.ontologicalCode.split('*')(0), this.technique, this.ontologicalCode.split('*')(1))
 }*/

}
