package it.polimi.genomics.metadata.util

import java.io.BufferedReader
import java.util.regex.Pattern
import java.util.stream.Collectors

import scala.collection.JavaConverters._

/**
 * Created by Tom on set, 2019
 *
 * Set of methods useful to identify strings that satisfy a pattern in files or other strings.
 */
object PatternMatch {

  /**
   * @param pattern a pattern instance. It can also be shared between threads as it's a read-only object.
   * @return a possibly empty list of strings, each one being a line from the file accessible through the BufferedReader
   *         instance given as argument
   */
  def getLinesMatching(pattern: Pattern, fromInputReader: BufferedReader): List[String] = {
    fromInputReader.lines.filter(pattern.asPredicate()).collect(Collectors.toList[String]).asScala.toList
  }

  /**
   * @param pattern a pattern instance. It can also be shared between threads as it's a read-only object.
   * @return If the pattern matches the input string, each group in the pattern identifies a part of the string. The
   *         parts are returned in a List in the same order as they are defined in the pattern. None otherwise.
   */
  def matchParts(input: String, pattern: Pattern): List[String] = {
    /*Notes for the reader about the usage of Matcher class. The docs are really cryptically written

    > matcher is like an iterator.
    > .matches() causes the matcher to evaluate if the regex applies to the whole string and to give an assignment to
    the groups.
    > .groupCount() tells how many groups exist in the pattern, not how many of them have been found in the input.
    In my case it's useless because I wrote the regex and I already know how many groups there are. So a call
    to .group(i) with i spanning from 1 to .groupCount() is safe only if .matches previously returned true, otherwise
    the groups will be null. The .group(0) returns the whole matching input.
    >.toMatchStatus is used to return an immutable picture of the status of the matcher, whose status mutates as
    a result to calls of methods find or group
    > .find is like "hasNext" and returns true until more occurrences, beside the initial one, of the whole pattern
    regex are found; false if the pattern doesn't match or if the matcher can't find any more occurrence
    > a group with quantifier n in a pattern, i.e. (something){n} isn't like repeating n times the group. In the second
    case, you'll be able to select each group with .group(i), while in the first, there'll be a single group with
    probably many instances... I don't know. The second way is easier.
    */
    val matcher = pattern.matcher(input)
    val res = if(matcher.matches())
      List.tabulate(matcher.groupCount())(i => matcher.group(i+1))
    else
      List.empty
    // debug
/*    println("THE FOLLOWING PARTS OF THE INPUT STRING HAVE BEEN RECOGNISED")
    if(res.isDefined)
      res.get.foreach(group => println(group))*/
    res
  }

  def createPattern(regex: String): Pattern = {
    Pattern.compile(regex)
  }
}
