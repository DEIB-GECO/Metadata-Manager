package it.polimi.genomics.metadata.util

import scala.collection.mutable.ListBuffer

/**
 * Created by Tom on ott, 2019
 *
 * This map models relations like K -> V+ and is optimal to represent many-to-many relations where many keys map to a
 * limited number of values with the smallest memory footprint. Even if the same value can be mapped to many keys,
 * internally the value is not replicated; instead the key is associated to a list of pointers and each pointer points
 * to a distinct value.
 *
 * K is a generic key
 * V is a generic value
 */
class ManyToFewMap[K, V] {

  private var keyToIndices: Map[K, ListBuffer[Int]] = Map.empty
  private var _values: ListBuffer[V] = ListBuffer.empty[V]

  /**
   * Adds a value to the specified key.
   */
  def add(key: K, value: V): Unit ={
    // add value to list of all values
    var valueIndex = _values.indexWhere(v => v.equals(value))  // get index if value is already in list
    if(valueIndex == -1) {  // value not listed yet
      valueIndex = _values.size
      _values += value
    }
    // add value to key's values
    if(keyToIndices.get(key).isDefined && !keyToIndices(key).contains(valueIndex)){ // key exists but index not
      keyToIndices += key -> (keyToIndices(key) += valueIndex)
    } else if(keyToIndices.get(key).isEmpty)  // key doesn't exists yet
      keyToIndices += key -> ListBuffer(valueIndex)
  }

  /**
   * If the key exists, it returns a List of values associated to this key, None otherwise.
   */
  def get(key: K): Option[List[V]] ={
    if(keyToIndices.get(key).isDefined)
      Some(keyToIndices(key).map(index => _values(index)).toList)
    else None
  }

  /**
   * If the key exists, it returns the first value associated to this key, None otherwise.
   * This method is useful if it's known that the key is mapped to only one value and it's equivalent to the
   * invocation get(key).get.head (given that the key exists)
   */
  def getFirstOf(key: K): Option[V] ={
    if(keyToIndices.get(key).isDefined)
      Some(keyToIndices(key).map(index => _values(index)).toList.head)
    else None
  }

  def keys: Iterable[K] ={
    keyToIndices.keys
  }

  def keySet: Set[K] ={
    keyToIndices.keySet
  }

  /**
   * For each key, it maps the values to a new value - as defined in the function passed as argument - and returns
   * a new scala.immutable.Map with the keys mapped to their new values.
   *
   */
  def toMap[A](function: (K, List[V]) => A):Map[K, A] ={
    val _keys = keys
    val transformedValues = _keys.toList.map(key => {
      val thisKeyValues = keyToIndices(key).map(index => _values(index)).toList
      function(key, thisKeyValues)
    })
    (_keys zip transformedValues).toMap
  }

  /**
   * Iterates through all the keys, for each key let the function map the List of its values to a new object, then
   * returns a new ManyToFewMap with each keys mapped to the new object returned by the function argument.
   */
  def transformValues[A](function: (K, List[V]) => A):ManyToFewMap[K, A] ={
    val resultMap = new ManyToFewMap[K, A]
    keys.foreach(key => {
      val thisKeyValues = keyToIndices(key).map(index => _values(index)).toList
      resultMap.add(key, function(key, thisKeyValues))
    })
    resultMap
  }

  /**
   * Iterates on all the keys, each time evaluating a key and the associated List of values.
   */
  def foreachKey(function: (K, List[V]) => Unit): Unit ={
    keyToIndices.foreach( tuple => {
      val thisKeyValues = tuple._2.map(i => _values(i)).toList
      function(tuple._1, thisKeyValues)
    })
  }

  /**
   * Iterates on all the mappings, each time evaluating a single key and a single value belonging to that key.
   */
  def foreach(function: (K, V) => Unit): Unit ={
    keyToIndices.foreach( tuple => {
      val thisKeyValues = tuple._2.map(i => _values(i))
      thisKeyValues.foreach(singleValue => {
        function(tuple._1, singleValue)
      })
    })
  }

  /**
   * Get all the distinct values that has been associated to any of the keys.
   */
  def values: List[V] ={
    _values.toList
  }

}
