package it.polimi.genomics.importer.ModelDatabase


class CaseItem extends EncodeTable{
  var itemId: Int = _

  var caseId: Int = _

  _hasForeignKeys = true

  _foreignKeysTables = List("ITEMS","CASES")

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = ???

  override def insertRow(): Unit ={
      if(this.checkInsert()) {
        this.insert
      }
  }

  override def insert() : Int ={
    dbHandler.insertCaseItem(itemId,caseId)
  }

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[Item])
      this.itemId = table.primaryKey
    if(table.isInstanceOf[Case])
      this.caseId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertCaseItem(this.itemId,this.caseId)

  }

  override def getId(): Int = {
   // dbHandler.getCasesItemId(this.itemId,this.caseId)
    -1
  }
}
