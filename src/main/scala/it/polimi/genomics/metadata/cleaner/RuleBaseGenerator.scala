package it.polimi.genomics.metadata.cleaner

import java.nio.file.{Files, Paths}

import scala.collection.mutable

object RuleBaseGenerator extends App {

  val all_keys_file = "all.txt"
  val unseen_keys_file = "unseen.txt"
  val seen_keys_file = "seen.txt"
  val rules_list_file = "rules.txt"

  if (args.length < 2) {
    println("This application needs 3 parameters to be run:\n " +
      "(1) path of datasets, inside of which the application will consider directories \"Transformations\",\n " +
      "(2) source (e.g., \"ENCODE\" or \"TCGA\")")
    // "(3) path of directory containing keys files and rules, may contain:\n" +
     // "- file \"" + seen_keys_file + "\" with keys seen until last run\n" +
     // "- file \"" + rules_list_file + "\" with already specified rules")
    //this directory will also be used for unseenkeys and all_keys (new File... .parent, non cercare in base a /)
    System.exit(0)
  }

  val base_path = args(0)
  //val cleaner_files_path = args(1)
  val source = args(1)

  //creation of RB
  val all_keys = IOManager.computeAllKeys(base_path, source)
  IOManager.writeKeys(base_path + source + "_" + all_keys_file, all_keys)

  val seen_keys_path = base_path + source + "_" + seen_keys_file
  val rule_list_path = base_path + source + "_" + rules_list_file

  val seen_keys: Option[mutable.LinkedHashSet[(String, String, Rule)]] = {
    if(Files.exists(Paths.get(seen_keys_path)))
      Some(IOManager.readSeenKeys(seen_keys_path))
    else None
  }

  val rule_list: Option[List[Rule]] = {
    if(Files.exists(Paths.get(rule_list_path)))
      Some(IOManager.readRules(rule_list_path))
    else None
  }

  RuleBase.createRB(rule_list, all_keys, seen_keys)


}
