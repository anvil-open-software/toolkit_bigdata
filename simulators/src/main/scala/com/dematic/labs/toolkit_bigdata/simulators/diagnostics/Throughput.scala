package com.dematic.labs.toolkit_bigdata.simulators.diagnostics

import java.time.Instant
import java.util
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import com.dematic.labs.toolkit_bigdata.simulators.configuration.MinimalProducerConfiguration
import com.dematic.labs.toolkit_bigdata.simulators.diagnostics.data.Signal
import com.dematic.labs.toolkit_bigdata.simulators.diagnostics.data.Utils.toJson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
  * Will push random signal objects as JSON to the configured Kafka Broker for a fixed about of time.
  *
  */
object Throughput extends App {
  val logger = LoggerFactory.getLogger("Throughput")

  // load all the configuration
  val config = new MinimalProducerConfiguration.Builder().build
  // define how long to run the throughput simulator
  val countdownTimer = new CountdownTimer
  countdownTimer.countDown(config.getDurationInMinutes.toInt)

  // generated ids
  private val nextId = {
    var id: Long = 1
    () => {
      id += 1
      id
    }
  }
  // generated values
  private val nextRandomValue = {
    val random = new Random
    () => {
      val num = random.nextInt()
      if (num < 0) num * -1 else num
    }
  }

  // number of threads on the box
  val numWorkers = sys.runtime.availableProcessors
  // underlying thread pool with a fixed number of worker threads, backed by an unbounded LinkedBlockingQueue[Runnable]
  // define a DiscardPolicy to silently passes over RejectedExecutionException
  val executorService = new ThreadPoolExecutor(numWorkers, numWorkers, 0L, TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable], Executors.defaultThreadFactory, new DiscardPolicy)

  logger.info(s"Producer using '$numWorkers' workers" )

  // the ExecutionContext that wraps the thread pool
  implicit val ec = ExecutionContext.fromExecutorService(executorService)

  // configure and create kafka producer
  val properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializer)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializer)
  properties.put(ProducerConfig.ACKS_CONFIG, config.getAcks)
  properties.put(ProducerConfig.RETRIES_CONFIG, Predef.int2Integer(config.getRetries))

  val producer = new KafkaProducer[String, AnyRef](properties)

  // fire and forget, until timer is finished
  try {
    while (!countdownTimer.isFinished) {
      val result = Future {
        // create random json
        val json = toJson(new Signal(nextId(), Instant.now.toString, nextRandomValue(), config.getId))
        producer.send(new ProducerRecord[String, AnyRef](config.getTopics, json))
      }
      // only print exception if, something goes wrong
      result onFailure {
        case any => logger.error("Unexpected Error:", any)
      }
    }
  } finally {
    // close execution context
    ec.shutdownNow()
    // close producer
    producer.close()
    val lastId = nextId() - 1
    logger.info(s"Completed: pushed '$lastId'")
  }
}