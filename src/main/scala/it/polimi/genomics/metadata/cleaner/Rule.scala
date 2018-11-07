package it.polimi.genomics.metadata.cleaner


import dk.brics.automaton._

import it.polimi.genomics.metadata.cleaner.IOManager._



case class Rule(antecedent: String, consequent: String) extends PartiallyOrdered[Rule] {
  //val pattern = antecedent.r

  override def toString: String = {
    antecedent + "=>" + consequent
  }

  override def tryCompareTo[B >: Rule](that: B)(implicit evidence$1: B => PartiallyOrdered[B]): Option[Int] = {
    if (that.isInstanceOf[Rule]) {
      val thatIns = that.asInstanceOf[Rule]

      val autoThis = new RegExp(this.antecedent).toAutomaton()
      autoThis.expandSingleton()
      autoThis.determinize()

      val autoThat = new RegExp(thatIns.antecedent).toAutomaton()
      autoThat.expandSingleton()
      autoThat.determinize()

      if (autoThis.equals(autoThat)) //intersection between L(a) and L(a') corresponds to L(a) and L(a')
        Some(0)
      else {
        val inter = BasicOperations.intersection(autoThis, autoThat)

        if(inter.isEmpty) //intersection between L(a) and L(a') is empty -> incomparable
          Some(-2)
        else{
          inter.determinize()
          if (inter.equals(autoThat)) //L(a') is contained in L(a) (new is contained in existing)
            Some(1)
          else if (inter.equals(autoThis)) //L(a) is contained in L(a') (existing is contained in new)
            Some(-1)
          else //intersection between L(a) and L(a') is partial
            Some(2)
        }
      }
    } else None

  }

}

//companion object
object Rule {

  //costructor
  def apply(pair: (String, String), order: Int) = new Rule(pair._1, pair._2)

  def StringToRule(s: String): Rule = {
    val rule_pattern = "(.*)=>(.*)"
    val r = new Rule(s.replaceFirst(rule_pattern, "$1"), s.replaceFirst(rule_pattern, "$2"))
    r
  }

  def simulateRule(key: String, r: Rule): Option[String] = {

    if (key.matches(r.antecedent)) {
      try {
        if (r.consequent.equals(""))
          Some(key.replaceAll(r.antecedent, ""))
        else
          Some(key.replaceAll(r.antecedent, r.consequent))
      }
      catch {
        case e: Exception => println("Rule has wrong syntax!"); throw e
      }
    }
    else
      None
  }

  def addRule(newRule: Rule, ruleList: List[Rule]): List[Rule] = {

    for (rule <- ruleList) {
      val rel = rule.tryCompareTo(newRule)
      if (rel.isDefined) { //new rule is comparable with current in ruleList
        if (rel.get == 0) { //new rule is equivalent or identical to already existing rule
          if (newRule != rule && keepNewRuleChoice(rule, newRule)) {
            return ruleList.updated(ruleList.indexOf(rule), newRule) //replace old with new rule
          }
          else
            return ruleList //return same list
        }
        else if(rel.get == 2){ //new rule has overlap with already existing rule
          if (prioritizeNewRuleChoice(rule,newRule)){
            val tempOverlap = ruleList.splitAt(ruleList.indexOf(rule))
            return tempOverlap._1 ::: List(newRule) ::: tempOverlap._2
          }
          else {}//keep browsing ruleList
        }
        else if (rule > newRule) { //the right position was passed; insert here
          val temp = ruleList.splitAt(ruleList.indexOf(rule))
          return temp._1 ::: List(newRule) ::: temp._2 //https://stackoverflow.com/questions/12600863/scala-convert-list-of-lists-into-a-single-list-listlista-to-lista
        }
        // if(rule.tryCompareTo(newRule) == -1){   }
        //} else { //new rule is comparable with current or is less than
      }
    }
    ruleList ::: List(newRule)
  }


}




