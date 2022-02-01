package it.polimi.genomics.metadata.mapper

trait Ancestry extends Table{

  var cohortId: Int = _

  var broadAncestralCategory : String = _

  var countryOfOrigin: String = _

  var countryOfRecruitment: String = _

  var numberOfIndividuals: Int = _

  var sourceId : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("COHORTS")

  override def insert(): Int = {
    dbHandler.insertAncestry(this.cohortId, this.broadAncestralCategory, this.countryOfOrigin, this.countryOfRecruitment, this.numberOfIndividuals, this.sourceId)
  }

  override def update(): Int = {
    dbHandler.updateAncestry(this.cohortId, this.broadAncestralCategory, this.countryOfOrigin, this.countryOfRecruitment, this.numberOfIndividuals, this.sourceId)
  }

  override def updateById(): Unit = {
    dbHandler.updateAncestryById(this.primaryKey, this.cohortId, this.broadAncestralCategory, this.countryOfOrigin, this.countryOfRecruitment, this.numberOfIndividuals, this.sourceId)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.cohortId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertAncestry(this.cohortId, this.broadAncestralCategory, this.countryOfOrigin, this.countryOfRecruitment, this.numberOfIndividuals)
  }

  /*
  override def getId(): Int = {
    dbHandler.getCaseId(this.sourceId)
  }*/

  override def checkConsistency(): Boolean = {
    if(this.broadAncestralCategory != null) true else false
  }

}
