package it.polimi.genomics.importer.ModelDatabase

class DerivedFrom extends EncodeTable {

  var initialItemId: Int = _

  var finalItemId: Int = _

  var description: String = ""

  override def getId: Int = ???

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String): Unit = ???

  override def checkInsert(): Boolean = {
    dbHandler.checkInsertDerivedFrom(initialItemId,finalItemId)
  }

  override def insert(): Int = {
    dbHandler.insertDerivedFrom(initialItemId,finalItemId,description)
  }

  override def update(): Int = {
    dbHandler.updateDerivedFrom(initialItemId,finalItemId,description)
  }

  override def setForeignKeys(table: Table): Unit = ???
}
