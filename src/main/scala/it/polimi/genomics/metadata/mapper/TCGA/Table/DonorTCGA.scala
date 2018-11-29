package it.polimi.genomics.metadata.mapper.TCGA.Table

import it.polimi.genomics.metadata.mapper.Donor

import scala.util.Try

class DonorTCGA extends TCGATable with Donor{

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit ={
    dest.toUpperCase() match {
      case "SOURCEID" => this.sourceId = insertMethod(this.sourceId, param)
      case "SPECIES" => this.species = insertMethod(this.species, param)
      case "AGE" => {
        val optAgeToString: Option[String] = this.age.map(_.toString)
        val ageToString: String = optAgeToString.getOrElse(null)

        val rightAge: String = insertMethod(ageToString,param)

        this.age = Try(rightAge.toInt).toOption //converts to a Some(Int) if possible, otherwise None
        println
      }
      case "GENDER" => this.gender = insertMethod(this.gender, param)
      case "ETHNICITY" => this.ethnicity = insertMethod(this.ethnicity, param)
      case _ => noMatching(dest)
    }
  }
}