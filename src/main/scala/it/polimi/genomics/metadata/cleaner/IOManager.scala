package it.polimi.genomics.metadata.cleaner

import java.io._
import java.nio.file.{Files, Paths}

import scala.collection.mutable.{ArrayBuffer, LinkedHashSet}
import scala.io.Source
import scala.io.StdIn.readLine
import it.polimi.genomics.metadata.cleaner.RuleBaseGenerator._


object IOManager {

  def computeAllKeys(dir: String, source: String): LinkedHashSet[String] = {

    var input_files = ArrayBuffer[File]()
    val folders = Utils.getListOfSubDirectories(dir)
    for (folder: String <- folders) {
      val f = new File(dir + folder)
      if (f.getName.toLowerCase.contains("_" + source.toLowerCase)) {
        val datasets = Utils.getListOfSubDirectories(dir + folder)
        for (dataset: String <- datasets) {
          val d = new File(dir + folder + "/" + dataset + "/Transformations")
          println("*****d="+d)
          val files = Utils.getListOfMetaFiles(d)
          for(i<-files)
            input_files += i
        }
      }
    }

    val output_file_lines = new LinkedHashSet[String]()

    try {
      for (current_file <- input_files) {
        val bufferedSource = Source.fromFile(current_file)
        for (line <- bufferedSource.getLines.toList) {
          output_file_lines += Utils.extractKey(line)//.replaceAll("__[0-9]*__", "__X__")
        }
        bufferedSource.close
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find that file")
      case e: IOException => println("Got an IOException!")
    }

    output_file_lines
  }

  def writeKeys(file_name: String, set: LinkedHashSet[String]): Unit = {
    val base_file: File = new File(file_name)
    val bw = new BufferedWriter(new FileWriter(base_file))
    val sorted_set = collection.immutable.SortedSet[String]() ++ set
    for (s <- sorted_set) {
      bw.write(s + "\n")
    }
    bw.close()
  }

  def writeSeenKeys(file_name: String, set: LinkedHashSet[(String, String, Rule)]): Unit = {
    val base_file: File = new File(file_name)
    val bw = new BufferedWriter(new FileWriter(base_file))
    //bw.write("%70s\t%70s\t%50s\n".format("Key before", "Key after", "Applied rule"))
    for (s <- set) {
      bw.write(getSeenKeysLine(s))
      //bw.write("%70s\t%70s\t%50s\n".format(s._1, s._2, s._3))
    }
    bw.close()
  }

  def writeRules(file_name: String, lis: List[Rule]): Unit = {
    val base_file: File = new File(file_name)
    val bw = new BufferedWriter(new FileWriter(base_file))
    for (s <- lis) {
      bw.write(s + "\n")
    }
    bw.close()
  }

  def getSeenKeysLine(t: (String, String, Rule)): String = {
    "Key before: " + t._1 + ",\tKey after: " + t._2 + ",\tApplied rule: " + t._3 + "\n"
  }

  //to print seen_keys file formatted nicely as a table (but not working properly!)
  /* def readSeenKeys(file_name: String): LinkedHashSet[(String, String, Rule)] = {
     val output_file_lines = new LinkedHashSet[(String, String, Rule)]()
     try {
       val bufferedSource = Source.fromFile(file_name)
       val rule_pattern = "(.*)\\t(.*)\\t(.*)=>(.*)"

       for (line <- bufferedSource.getLines) {
         val line_ns = line.replaceAll("\\t\\s*", "\\t")
         output_file_lines += ((line_ns.replaceFirst(rule_pattern, "$1"), line_ns.replaceFirst(rule_pattern, "$2"), new Rule(line_ns.replaceFirst(rule_pattern, "$3"), line_ns.replaceFirst(rule_pattern, "$4"))))
       }
       bufferedSource.close
     } catch {
       case e: FileNotFoundException => println("Couldn't find file " + file_name)
       case e: IOException => println("Got an IOException!")
     }
     output_file_lines
   }
   */

  def readSeenKeys(file_name: String): LinkedHashSet[(String, String, Rule)] = {
    val output_file_lines = new LinkedHashSet[(String, String, Rule)]()
    try {
      val bufferedSource = Source.fromFile(file_name)
      val line_pattern = "Key before: (.*),\tKey after: (.*),\tApplied rule: (.*)=>(.*)"

      for (line <- bufferedSource.getLines) {
        // val line_ns = line.replaceAll("\\t\\s*", "\\t")
        output_file_lines += ((line.replaceFirst(line_pattern, "$1"), line.replaceFirst(line_pattern, "$2"), new Rule(line.replaceFirst(line_pattern, "$3"), line.replaceFirst(line_pattern, "$4"))))
      }
      bufferedSource.close
    } catch {
      case e: FileNotFoundException => println("Couldn't find file " + file_name)
      case e: IOException => println("Got an IOException!")
    }
    output_file_lines
  }

  def readRules(file_name: String): List[Rule] = {
    var rulesList = List[Rule]()
    try {
      val bufferedSource =
        if (new File(file_name).exists)
          Source.fromFile(file_name)
        else
          Source.fromFile(file_name)
      for (line <- bufferedSource.getLines) {
        rulesList = rulesList ::: List(Rule.StringToRule(line))
      }
      bufferedSource.close
    } catch {
      case e: FileNotFoundException => println("Couldn't find file " + file_name)
      case e: IOException => println("Got an IOException!")
    }
    rulesList
  }

  def printWelcomeMsg(): Unit = {
    println("\nPlease open the \"" + unseen_keys_file + "\" and \"" + rules_list_file + "\" files and get inspiration for new cleaning rules!")
  }

  def getRuleOrQuitChoice: String = {

    print("\nPress R (rule) to insert rule, Q (quit) to quit input procedure: ")

    val line = readLine()
    line match {
      case "r" | "R" => println("Insert new rule to clean keys (with syntax antecedent=>consequent):"); line
      case "q" | "Q" => println("RuleBaseGenerator is quitting... Goodbye!"); line
      case _ => println("Error, your choice is not valid. Choose again: "); getRuleOrQuitChoice
    }
  }

  def getRejectOrAcceptChoice: String = {
    val line = readLine()
    line match {
      case "n" | "N" => print("You chose to reject the specified rule! "); line
      case "y" | "Y" => println("\nYou accepted the rule! Find the changes (if any) in \"" + seen_keys_file + "\" and \"" + rules_list_file + "\"\n"); line
      case _ => println("Error, your choice is not valid."); getRejectOrAcceptChoice
    }
  }

  def getRuleFromUser: (String, String) = {
    try {
      val ExpectedPatternRule = "(.*)=>(.*)".r
      val ExpectedPatternRule(a, c) = readLine()
      (a, c)
    } catch {
      case e: Exception => println("Input is not a rule!"); throw e
    }
  }

  def keepNewRuleChoice(oldRule: Rule, newRule: Rule): Boolean = {

    print("\nThe new rule antecedent is identical or equivalent to an existing one." +
      "\nOld (O): " + oldRule +
      "\nNew (N): " + newRule +
      "\nTo choose new rule press N, to keep the old rule press O: ")

    val line = readLine()
    line match {
      case "n" | "N" => println("You chose to change the rule!\n"); true
      case "o" | "O" => println("You chose to keep the old rule!\n"); false
      case _ => println("Error, your choice is not valid.\n"); keepNewRuleChoice(oldRule, newRule)
    }
  }

  def updateFiles(ruleList: List[Rule], unseen_keys: LinkedHashSet[String], seen_keys: LinkedHashSet[(String, String, Rule)]): Unit = {
    val pr = Paths.get(rule_list_path)
    if (!Files.exists(pr)) Files.createFile(pr)
    writeRules(rule_list_path, ruleList)

    val ps = Paths.get(seen_keys_path)
    if (!Files.exists(ps)) Files.createFile(ps)
    writeSeenKeys(seen_keys_path, seen_keys)

    writeKeys(base_path + source + "_" + unseen_keys_file, unseen_keys)
  }

}
