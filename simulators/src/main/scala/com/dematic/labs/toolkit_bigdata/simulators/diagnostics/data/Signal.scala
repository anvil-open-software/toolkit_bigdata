package com.dematic.labs.toolkit_bigdata.simulators.diagnostics.data

import scala.beans.BeanProperty
import scala.util.hashing.MurmurHash3

class Signal(@BeanProperty var id: Long, @BeanProperty var timestamp: String, @BeanProperty var value: Int,
             @BeanProperty val producerId: String) {
  override def equals(other: Any): Boolean = other match {
    case that: Signal => (that canEqual this) &&
      that.id == this.id &&
      that.timestamp == this.timestamp &&
      that.value == this.value &&
      that.producerId == this.producerId
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Signal]

  override def hashCode(): Int = MurmurHash3.seqHash(List(id, timestamp, value, producerId))

  override def toString = s"Signal($id, $timestamp, $value , $producerId)"
}