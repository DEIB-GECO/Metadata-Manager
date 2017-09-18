package it.polimi.genomics.importer.ModelDatabase


class Project extends EncodeTable{

    var projectName: String = _

    var programName: String = _

    override def setParameter(param: String, dest: String): Unit = dest.toUpperCase() match {
      case "PROJECTNAME" => this.projectName = setValue(this.projectName,param)
      case "PROGRAMNAME" => this.programName = setValue(this.programName,param)
      case _ => noMatching(dest)
    }
}
