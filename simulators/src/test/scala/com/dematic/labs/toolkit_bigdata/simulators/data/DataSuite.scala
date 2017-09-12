package com.dematic.labs.toolkit_bigdata.simulators.data

import java.time.Instant

import com.dematic.labs.toolkit_bigdata.simulators.data.SignalType.Sorter
import org.scalatest.FunSuite

class DataSuite extends FunSuite {
  test("signal to json, json to signal") {
    val signal = new Signal(123, Instant.now().toString, Sorter, 5555, "DataSuite")
    val toJson = Utils.toJson(signal)
    val fromJson = Utils.fromJson[Signal](toJson)
    assert(signal === fromJson)
  }
}
