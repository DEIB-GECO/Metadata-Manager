package it.polimi.genomics.importer.cleaner




object Cleaner extends App{

  /*val input_directory_path = "/Users/abernasconi/Documents/gitProjects/importer_data/input_files"
  val output_directory_path = "/Users/abernasconi/Documents/gitProjects/importer_data/cleaned_files/"
  val rules_file = "rules.txt"
  val all_keys_file = "all_keys.txt"
  val unseen_keys_file = "unseen_keys.txt"
  val seen_keys_file = "seen_keys.txt"*/

   if (args.length < 6) {
      println("This jar needs 6 parameters to be run: input_directory_path, output_directory_path, rules_file, all_keys_file, unseen_keys_file, seen_keys_file")
    }

    val input_directory_path = args(0)
    val output_directory_path = args(1)
    val rules_file = args(2)
    val all_keys_file = args(3)
    val unseen_keys_file = args(4)
    val seen_keys_file = args(5)


    //application of RB
    RuleBase.applyRB(rules_file, input_directory_path, output_directory_path)


}
