package com.dematic.labs.toolkit_bigdata.simulators.diagnostics

import java.time.Instant
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import com.dematic.labs.toolkit_bigdata.simulators.configuration.MinimalProducerConfiguration

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
  * Will push random signal objects as JSON to the configured Kafka Broker for a fixed about of time.
  *
  */
object Throughput extends App {
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

  // the ExecutionContext that wraps the thread pool
  implicit val ec = ExecutionContext.fromExecutorService(executorService)

  // fire and forget, until timer is finished
  //val producer = new Producer
  try {
    while (!countdownTimer.isFinished) {
      val result = Future {
        // create random json
       // val json = toJson(new Signal(nextId(), Instant.now.toString, nextRandomValue(), config.getId))
        //producer.send(json)
      }
      // only print exception if, something goes wrong
      result onFailure {
        case t => println(s"Unexpected Error:\n ${t.printStackTrace()}")
      }
    }
  } finally {
    // close execution context
    ec.shutdownNow()
    // close producer
  //  producer.close()
  }
}