package it.polimi.genomics.metadata.util

import scala.collection.mutable.ListBuffer

/**
 * Created by Tom on ott, 2019
 *
 * A Map of pairs key -> values optimal to represent many-to-many relations where many keys can map to a limited
 * number of values. Even if the same value can be mapped to many keys, internally the value is not replicated; instead
 * the key is associated to a list of pointers and each pointer points to a value.
 *
 * K is a generic key
 * V is a generic value
 * This map models relations like K -> V*
 */
class ManyToFewMap[K, V] {

  private var keyToIndices: Map[K, ListBuffer[Int]] = Map.empty
  private var _values: ListBuffer[V] = ListBuffer.empty[V]

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

  def get(key: K): Option[List[V]] ={
    if(keyToIndices.get(key).isDefined)
      Some(keyToIndices(key).map(index => _values(index)).toList)
    else None
  }

  def keys: Iterable[K] ={
    keyToIndices.keys
  }

  def keySet: Set[K] ={
    keyToIndices.keySet
  }

  def map[A](function: (K, List[V]) => A):Map[K, A] ={
    val _keys = keys
    val transformedValues = _keys.toList.map(key => {
      val thisKeyValues = keyToIndices(key).map(index => _values(index)).toList
      function(key, thisKeyValues)
    })
    (_keys zip transformedValues).toMap
  }
  
  def values: List[V] ={
    _values.toList
  }

}
