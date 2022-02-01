package it.polimi.genomics.metadata.mapper

trait Cohort extends Table{

  var itemId: Int = _

  var traitName : String = _

  var caseNumber_initial : Int = _

  var controlNumber_initial: Int = _

  var individualNumber_initial: Int = _

  var triosNumber_initial: Int = _

  var caseNumber_replicate : Int = _

  var controlNumber_replicate: Int = _

  var individualNumber_replicate: Int = _

  var triosNumber_replicate: Int = _

  var sourceId : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("ITEMS")

  override def insert(): Int = {
    dbHandler.insertCohort(this.itemId, this.traitName, this.caseNumber_initial, this.controlNumber_initial, this.individualNumber_initial, this.triosNumber_initial,
    this.caseNumber_replicate, this.controlNumber_replicate, this.individualNumber_replicate, this.triosNumber_replicate, this.sourceId)
  }

  override def update(): Int = {
    dbHandler.updateCohort(this.itemId, this.traitName, this.caseNumber_initial, this.controlNumber_initial, this.individualNumber_initial, this.triosNumber_initial,
      this.caseNumber_replicate, this.controlNumber_replicate, this.individualNumber_replicate, this.triosNumber_replicate, this.sourceId)
  }

  override def updateById(): Unit = {
    dbHandler.updateCohortById(this.primaryKey, this.itemId, this.traitName, this.caseNumber_initial, this.controlNumber_initial, this.individualNumber_initial, this.triosNumber_initial,
                                                                             this.caseNumber_replicate, this.controlNumber_replicate, this.individualNumber_replicate, this.triosNumber_replicate, this.sourceId)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.itemId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertCohort(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getCohortId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }

}
