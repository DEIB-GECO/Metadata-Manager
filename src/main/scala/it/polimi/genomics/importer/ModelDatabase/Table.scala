package it.polimi.genomics.importer.ModelDatabase

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.importer.ModelDatabase.Exception.NoTupleInDatabaseException
import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer


trait Table {

  val logger: Logger = Logger.getLogger(this.getClass)
  protected val conf = ConfigFactory.load()

  private val prefix = conf.getString("export.prefix")
  private val separation = conf.getString("export.separation")

  protected val dbHandler = DbHandler

  protected var _hasForeignKeys: Boolean = false
  protected var _foreignKeysTables: List[String] = _

  protected var _hasDependencies: Boolean = false
  protected var _dependenciesTables: List[String] = _

  def hasForeignKeys: Boolean = _hasForeignKeys
  def foreignKeysTables: List[String] = _foreignKeysTables

  def dependenciesTables: List[String] = _dependenciesTables
  def hasDependencies: Boolean = _hasDependencies
  def checkDependenciesSatisfaction(table: Table): Boolean = true
  def getDependencies(): Set[Any] = null


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
  def updateById(): Unit = {}

  def setForeignKeys(table: Table): Unit

  /*def insertRow(): Unit ={
    if(this.checkInsert()) {
      val id = this.insert
      this.primaryKey_(id)
    }
    else {
      //val id = this.getId
      val id = this.update
      this.primaryKey_(id)
    }
  }*/

  def insertRow(): Unit = {
    val id = this.getId
    if(id == -1) {
      val newId = this.insert
      this.primaryKey_(newId)
    } else {
      this.primaryKey_(id)
      this.updateById()
    }

  }

  def checkConsistency(): Boolean = {
    true
  }

  protected var _filePath: String = _

  def filePath: String = _filePath
  def filePath_: (filePath: String): Unit = this._filePath = filePath
  def getWriter(path: String): PrintWriter = new PrintWriter(new FileOutputStream(new File(path),true))
  def getMessage(tableName: String, attribute: String, value: Any): String = this.prefix + this.separation + tableName + this.separation + attribute + "\t" + value + "\n"
  def getMessageMultipleAttribute(value: Any, values: String*): String = values.fold(this.prefix){(acc,i) => acc + this.separation + i} + "\t" + value + "\n"
  def flushAndClose(write: PrintWriter): Unit ={
    write.flush()
    write.close()
  }

  def checkValueLength(values: Seq[Any]): Unit = {
    if(values.length == 0){
      throw new NoTupleInDatabaseException("Not tuple found Exception")
    }
  }
}
