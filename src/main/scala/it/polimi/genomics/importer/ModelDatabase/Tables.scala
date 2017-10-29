package it.polimi.genomics.importer.ModelDatabase

import scala.collection.mutable


trait Tables extends Enumeration{


  protected val tables: mutable.Map[Value, Table] = collection.mutable.Map[this.Value, Table]()

  def selectTableByName(name: String): Table
  def selectTableByValue(enum: this.Value): Table
  }
