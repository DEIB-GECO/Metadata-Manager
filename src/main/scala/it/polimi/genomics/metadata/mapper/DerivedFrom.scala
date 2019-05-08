package it.polimi.genomics.metadata.mapper

trait DerivedFrom extends Table{

  var initialItemId: Int = _

  var finalItemId: Int = _

  var description: String = _

  override def getId: Int = ???


 /* override def checkInsert(): Boolean = {
    dbHandler.checkInsertDerivedFrom(initialItemId,finalItemId)
  }

  override def insert(): Int = {
    dbHandler.insertDerivedFrom(initialItemId,finalItemId,description)
  }

  override def update(): Int = {
    dbHandler.updateDerivedFrom(initialItemId,finalItemId,description)
  }
  */

  override def setForeignKeys(table: Table): Unit = ???

}
