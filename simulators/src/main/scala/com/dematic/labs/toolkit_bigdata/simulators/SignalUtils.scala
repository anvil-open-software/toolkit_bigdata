package com.dematic.labs.toolkit_bigdata.simulators

import java.time.Instant
import java.util

import com.dematic.labs.toolkit_bigdata.simulators.data.Signal
import com.dematic.labs.toolkit_bigdata.simulators.data.SignalType.Sorter
import com.dematic.labs.toolkit_bigdata.simulators.data.Utils.toJson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Used in unit test to help send signals to kafka.
  *
  * @param bootstrapServer      -- server address
  * @param topic                -- kafka topic
  * @param numberOfSignalsPerId -- number of signals per id to send
  * @param signalIdRange        -- signal Id range
  * @param id                   -- test id
  */
class SignalUtils(val bootstrapServer: String, val topic: String, val numberOfSignalsPerId: Int,
                  val signalIdRange: Seq[Integer], val id: String) {
  private val properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.put(ProducerConfig.ACKS_CONFIG, "all")

  // generated values
  private val nextRandomValue = {
    val random = new Random
    () => {
      val num = random.nextInt()
      if (num < 0) num * -1 else num
    }
  }

  private val producer = new KafkaProducer[String, AnyRef](properties)

  try {
    // convert to Int and cycle through the range
    var lowSignalRange: Int = signalIdRange.head
    var highSignalRange: Int = signalIdRange.last

    for (i <- lowSignalRange to highSignalRange) {
      // number of signals to send
      for (i <- 1 to numberOfSignalsPerId) {
        val json = toJson(new Signal(i, Instant.now.toString, Sorter, nextRandomValue(), id))
        producer.send(new ProducerRecord[String, AnyRef](topic, json))
      }
    }
  } finally {
    producer.close()
  }
}