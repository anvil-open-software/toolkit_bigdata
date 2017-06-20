package com.dematic.labs.toolkit_bigdata.simulators.grainger;

import com.dematic.labs.toolkit_bigdata.simulators.configuration.grainger.OpcTagReaderConfiguration;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class OpcTagReadingExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpcTagReadingExecutor.class);

    // EX: 100 110 30 3 10.102.20.10:9092,10.102.20.11:9092 mm_test 10.102.20.30 mm_test sparkUserId sparkPassword test_app
    private static final String HELP = "OpcTagReadingExecutor " +
            "opcTagRangeMin opcTagRangeMax maxSignalsPerMinutePerOpcTag durationInMinutes kafkaServerBootstrap kafkaTopics " +
            "ApplicationName generatorId";


    private final int opcTagRangeSize;
    private final Stream<String> opcTagRangeIds;
    private final int maxSignalsPerMinutePerOpcTag;
    private final String generatorId;

    private OpcTagReadingExecutor(final int opcTagRangeMin, final int opcTagRangeMax,
                                  final int maxSignalsPerMinutePerOpcTag, final String generatorId) {
        opcTagRangeSize = opcTagRangeMax - opcTagRangeMin;
        opcTagRangeIds = IntStream.range(opcTagRangeMin, opcTagRangeMax).mapToObj(String::valueOf);
        this.maxSignalsPerMinutePerOpcTag = maxSignalsPerMinutePerOpcTag;
        this.generatorId = generatorId;
        LOGGER.info("OpcTagReadingExecutor: created with a opcTagRangeSize {} between {} and {} with " +
                        "maxSignalsPerMinutePerOpcTag {} and generatorId {}", opcTagRangeSize, opcTagRangeMin,
                opcTagRangeMax, maxSignalsPerMinutePerOpcTag, generatorId);
    }

    private void execute(final Long durationInMinutes, final String kafkaServerBootstrap, final String kafkaTopics) {
        final CountDownLatch latch = new CountDownLatch(opcTagRangeSize);
        final ForkJoinPool forkJoinPool =
                new ForkJoinPool(opcTagRangeSize, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        try {
            opcTagRangeIds.forEach(opcTagId -> forkJoinPool.submit(() ->
                    dispatchPerOpcTagReading(kafkaServerBootstrap, kafkaTopics, opcTagId, durationInMinutes, latch)));
            // wait 5 minutes longer then duration
            latch.await(durationInMinutes + 5, TimeUnit.MINUTES);
        } catch (final Throwable any) {
            LOGGER.error("OpcTagReadingExecutor: Unhandled Error: stopping execution", any);
        } finally {
            try {
                forkJoinPool.shutdownNow();
            } catch (final Throwable ignore) {
            }
        }
    }

    private void dispatchPerOpcTagReading(final String kafkaServerBootstrap, final String kafkaTopics,
                                          final String opcTagId, final Long durationInMinutes, final CountDownLatch latch) {
        LOGGER.debug("OpcTagReadingExecutor: Dispatching signals for {}", opcTagId);

        /*try (final KafkaProducer<String, byte[]> kafkaProducer = getKafkaProducer(kafkaServerBootstrap)) {
            // generate signals for the specific amount of time in minutes for a specific opc tag reading
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown((int) TimeUnit.MINUTES.toMinutes(durationInMinutes));

            final RateLimiter rateLimiter = RateLimiter.create(signalsPerSecond(maxSignalsPerMinutePerOpcTag));
            final Random randomNumberGenerator = new Random();

            while (true) {
                // dispatchSingleSignal until duration ends at a rate specified by max signal
                dispatchSingleSignal(kafkaProducer, kafkaTopics, opcTagId, rateLimiter, randomNumberGenerator);

                if (countdownTimer.isFinished()) {
                    break;
                }
            }
        } finally {
            latch.countDown();
        }*/
    }

    private void dispatchSingleSignal(final KafkaProducer<String, byte[]> kafkaProducer, final String kafkaTopics,
                                      final String opcTagId, final RateLimiter rateLimiter,
                                      final Random randomNumberGenerator) {
        rateLimiter.acquire();
        // signals will just be created from json string provided by grainger and updated OPCTagID, Timestamp, Value
        final String timestamp = Instant.now().toString(); // 2016-03-03T19:13:13.3980463Z
        final long value = nextRandomNumber(randomNumberGenerator);
        final String signal = String.format(" [{\n" +
                " \"ExtendedProperties\":[\"%s\"],\n" +
                " \"ProxiedTypeName\":\"Odatech.Business.Integration.OPCTagReading\",\n" +
                " \"OPCTagID\":%s,\n" +
                " \"OPCTagReadingID\":0,\n" +
                " \"Quality\":192,\n" +
                " \"Timestamp\":\"%s\",\n" +
                " \"Value\":\"%s\",\n" +
                " \"ID\":0,\n" +
                " \"UniqueID\":null\n" +
                " }]", generatorId, opcTagId, timestamp, value);

        // for now signal time and value are just randomly generated
        try {
            final Future<RecordMetadata> send =
                    kafkaProducer.send(new ProducerRecord<>(kafkaTopics, signal.getBytes(Charset.defaultCharset())));
            // get will wait until a response
            final RecordMetadata recordMetadata = send.get();
            LOGGER.debug("OpcTagReadingExecutor: {} successfully sent >{}< to {}", opcTagId, signal,
                    recordMetadata.topic());
        } catch (final Throwable any) {
            LOGGER.error(String.format("OpcTagReadingExecutor: Unexpected error: dispatching signal to >%s<",
                    kafkaTopics), any);
        }
    }

    private static double signalsPerSecond(final int signalsPerMinutes) {
        return (double) Math.round((signalsPerMinutes / 60d) * 100) / 100;
    }

    private static long nextRandomNumber(final Random randomNumberGenerator) {
        // to generate values with an average of 500 and a standard deviation of 100
        final double val = randomNumberGenerator.nextGaussian() * 25 + 1000;
        return Math.round(val);
    }

    public static void main(String[] args) {
        // configuration comes from the application.conf for the driver
        final OpcTagReaderConfiguration config = new OpcTagReaderConfiguration.Builder().build();

        try {
            final OpcTagReadingExecutor opcTagReadingExecutor = new OpcTagReadingExecutor(config.getOpcTagRangeMin(),
                    config.getOpcTagRangeMax(), config.getMaxSignalsPerMinutePerOpcTag(), config.getId());

            opcTagReadingExecutor.execute(config.getDurationInMinutes(), config.getBootstrapServers(),
                    config.getTopics());
        } finally {
            Runtime.getRuntime().halt(0);
        }
    }
}
