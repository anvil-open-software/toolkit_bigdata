package com.dematic.labs.toolkit_bigdata.simulators.diagnostics.data

import java.time.Instant

import org.scalatest.FunSuite

class DataSuite extends FunSuite {
  test("signal to json, json to signal") {
    val signal = new Signal(123, Instant.now().toString, 5555, "DataSuite")
    val toJson = Utils.toJson(signal)
    val fromJson = Utils.fromJson[Signal](toJson)
    assert(signal === fromJson)
  }
}
