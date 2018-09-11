package it.polimi.genomics.metadata.cleaner

import it.polimi.genomics.metadata.cleaner.IOManager.{computeAllKeys, readRules, readSeenKeys, writeKeys}


object RuleBaseGenerator extends App {

  /*val input_directory_path = "/Users/abernasconi/Documents/gitProjects/importer_data/input_files"
  val output_directory_path = "/Users/abernasconi/Documents/gitProjects/importer_data/cleaned_files/"
  val rules_file = "rules.txt"
  val all_keys_file = "all_keys.txt"
  val unseen_keys_file = "unseen_keys.txt"
  val seen_keys_file = "oldoldseen.txt"*/

  val all_keys_file = "all.txt"
  val unseen_keys_file = "unseen.txt"
  val seen_keys_file = "seen.txt"

  if (args.length < 3) {
    println("This application needs 3 parameters to be run:\n " +
      "(1) path of directory of transformed .meta files,\n " +
      "(2) path of file containing rules,\n " +
      "(3) path of directory containing keys files:\n" +
      "this can contain file \"" + seen_keys_file + "\" containing keys seen until last run") //this directory will also be used for unseekkeys and all_keys (new File... .parent, non cercare in base a /)
    System.exit(0)
  }

  val input_directory_path = args(0)
  val rules_file_path = args(1)
  val keys_dir_path = args(2)


  //creation of RB
  val all_keys = computeAllKeys(input_directory_path)
  writeKeys(keys_dir_path + all_keys_file, all_keys)
  val seen_keys = readSeenKeys(keys_dir_path + seen_keys_file)
  val ruleList = readRules(rules_file_path)
  RuleBase.createRB(ruleList, all_keys, seen_keys, keys_dir_path + unseen_keys_file)


  //application of RB
  //RuleBase.applyRB(rules_file, input_directory_path, output_directory_path)


}
