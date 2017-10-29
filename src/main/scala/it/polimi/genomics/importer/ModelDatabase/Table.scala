package it.polimi.genomics.importer.ModelDatabase

import scala.collection.mutable.ListBuffer


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

  protected var _primaryKeys: ListBuffer[Int] = new ListBuffer[Int]
  def primaryKeys:ListBuffer[Int] = _primaryKeys
  def primaryKeys_ (value: ListBuffer[Int]):Unit = _primaryKeys = value

  def getId: Int
  def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit

  def checkInsert():Boolean
  def insert(): Int
  def setForeignKeys(table: Table): Unit
  def insertRow(): Unit
}
