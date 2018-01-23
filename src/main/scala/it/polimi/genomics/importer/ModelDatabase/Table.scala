package it.polimi.genomics.importer.ModelDatabase

import java.io.{File, FileOutputStream, PrintWriter}

import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer


trait Table {

 /* protected var _primaryKey: Set[Int] = _
  def primaryKey = _primaryKey
  def primaryKey_= (value:Set[Int]):Unit = _primaryKey = value*/
 val logger: Logger = Logger.getLogger(this.getClass)

  protected val dbHandler = DbHandler

  protected var _hasForeignKeys: Boolean = false
  protected var _foreignKeysTables: List[String] = _
  def hasForeignKeys: Boolean = _hasForeignKeys
  def foreignKeysTables: List[String] = _foreignKeysTables

  protected var _primaryKey: Int = _
  def primaryKey: Int = _primaryKey
  def primaryKey_ (value:Int):Unit = _primaryKey = value

  protected var _primaryKeys: ListBuffer[Int] = new ListBuffer[Int]
  def primaryKeys:ListBuffer[Int] = _primaryKeys
  def primaryKeys_ (value: Int):Unit = _primaryKeys += value

  def getId: Int
  def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit

  def checkInsert():Boolean
  def insert(): Int
  def update(): Int
  def setForeignKeys(table: Table): Unit

  def insertRow(): Unit ={
    if(this.checkInsert()) {
      val id = this.insert
      this.primaryKey_(id)
    }
    else {
      //val id = this.getId
      val id = this.update
      this.primaryKey_(id)
    }
  }

  def checkConsistency(): Boolean = {
    true
  }

  protected var _filePath: String = _

  def filePath: String = _filePath
  def filePath_: (filePath: String): Unit = this._filePath = filePath
  def getWriter(path: String): PrintWriter = return new PrintWriter(new FileOutputStream(new File(path),true))
  def getMessage(attribute: String, value: Any): String = return "integrated__" + attribute + "\t" + value + "\n"
  def flushAndClose(write: PrintWriter): Unit ={
    write.flush()
    write.close()
  }
}
