package it.polimi.genomics.metadata.mapper.GWAS.Table

import it.polimi.genomics.metadata.mapper.{Ancestry, Table}
import it.polimi.genomics.metadata.mapper.GWAS.GwasTableId
import it.polimi.genomics.metadata.mapper.Utils.Statistics

import scala.collection.mutable.ListBuffer

class AncestryGwas(gwasTableId: GwasTableId, quantity: Int) extends GwasTable(gwasTableId) with Ancestry{

  var cohortIdList: ListBuffer[Int] = new ListBuffer[Int]

  var broadAncestralCategoryList : ListBuffer[String] = new ListBuffer[String]

  var countryOfOriginList: ListBuffer[String] = new ListBuffer[String]

  var countryOfRecruitmentList: ListBuffer[String] = new ListBuffer[String]

  var numberOfIndividualsList: ListBuffer[Int] = new ListBuffer[Int]

  var sourceIdList: ListBuffer[String] = new ListBuffer[String]

  _hasForeignKeys = true

  _foreignKeysTables = List("COHORTS")

  var actualPosition : Int = _

  def getId(): Int = {
    dbHandler.getAncestryId(this.broadAncestralCategoryList(this.actualPosition), this.countryOfOriginList(this.actualPosition), this.countryOfRecruitmentList(this.actualPosition), this.numberOfIndividualsList(this.actualPosition), this.sourceIdList(this.actualPosition))
  }

  override def setParameter(param: String, dest: String, insertMethod: (String, String) => String):  Unit =   dest.toUpperCase() match{
    case "SOURCEID" => this.sourceIdList += param
    case "BROADANCESTRALCATEGORY" => this.broadAncestralCategoryList += param
    case "COUNTRYOFORIGIN" => this.countryOfOriginList += param
    case "COUNTRYOFRECRUITMENT" => this.countryOfRecruitmentList += param
    case "NUMBEROFINDIVIDUALS" => {
      val tmp = param.toCharArray
      var bool = true
      for (i <- tmp){
        if (!i.isDigit) bool = false
      }
      if(!param.equals("NR") && bool) {
        if(numberOfIndividualsList.length == 1 && broadAncestralCategoryList.length == 0){
          //the following three lines needs for FinnGen
          numberOfIndividualsList(0) = numberOfIndividualsList(0) + param.toInt
          if(broadAncestralCategoryList.length < numberOfIndividualsList.length) broadAncestralCategoryList += "NR"
          if(countryOfOriginList.length < numberOfIndividualsList.length) countryOfOriginList += "NR"
          if(countryOfRecruitmentList.length < numberOfIndividualsList.length) countryOfRecruitmentList += "NR"
        } else this.numberOfIndividualsList += param.toInt
      }
        else this.numberOfIndividualsList += 0
    }
    case _ => noMatching(dest)
  }

  override def checkConsistency(): Boolean = {
    var flag = true
    for(i <- 0 until quantity){
      if(cohortIdList == null) flag = false
    }
    flag
  }

  override def insertRow(): Int ={
    var id: Int = 0
    for(i <- 0 until quantity){
      Statistics.ancestryInsertedOrUpdated += 1
      this.actualPosition = i
      var id = getId
      if (id == -1) {
        id = this.insert
        this.primaryKeys_(id)
      } else {
        this.primaryKeys_(id)
        this.updateById()
      }
    }
    id
  }

  override def insert(): Int = {
    this.dbHandler.insertAncestry(this.cohortIdList(this.actualPosition), this.broadAncestralCategoryList(this.actualPosition),this.countryOfOriginList(this.actualPosition),
                                  this.countryOfRecruitmentList(this.actualPosition), this.numberOfIndividualsList(this.actualPosition), this.sourceIdList(this.actualPosition))
  }

  override def setForeignKeys(table: Table): Unit = {
    for(i <- 0 until quantity) this.cohortIdList += table.primaryKey
  }
}
