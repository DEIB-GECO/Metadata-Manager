package it.polimi.genomics.importer.ModelDatabase


trait Table {

 /* protected var _primaryKey: Set[Int] = _
  def primaryKey = _primaryKey
  def primaryKey_= (value:Set[Int]):Unit = _primaryKey = value*/

  protected var _hasForeignKeys: Boolean = false
  protected var _foreignKeysTables: List[String] = _
  def hasForeignKeys: Boolean = _hasForeignKeys
  def foreignKeysTables: List[String] = _foreignKeysTables
  protected var _primaryKey: Int = _
  def primaryKey: Int = _primaryKey
  def primaryKey_ (value:Int):Unit = _primaryKey = value
  def getId: Int
  def setParameter(param: String, dest: String): Unit
 // def setValue(actualParam: String, newParam: String): String
  def checkInsert():Boolean
  def insert(): Int
  def setForeignKeys(table: Table): Unit
}
