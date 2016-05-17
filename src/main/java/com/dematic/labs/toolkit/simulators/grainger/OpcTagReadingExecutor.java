package com.dematic.labs.toolkit.simulators.grainger;

import com.dematic.labs.toolkit.CountdownTimer;
import com.dematic.labs.toolkit.simulators.Statistics;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.dematic.labs.toolkit.kafka.Connections.getKafkaProducer;

public final class OpcTagReadingExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpcTagReadingExecutor.class);

    private final int opcTagRangeSize;
    private final Stream<String> opcTagRangeIds;
    private final int maxSignalsPerMinutePerNode;
    private final String generatorId;
    private final Statistics statistics;

    private OpcTagReadingExecutor(final int opcTagRangeMin, final int opcTagRangeMax,
                                  final int maxSignalsPerMinutePerNode, final String generatorId) {
        opcTagRangeSize = opcTagRangeMax - opcTagRangeMin;
        opcTagRangeIds = IntStream.range(opcTagRangeMin, opcTagRangeMax).mapToObj(i -> generatorId + "-" + i);
        this.maxSignalsPerMinutePerNode = maxSignalsPerMinutePerNode;
        this.generatorId = generatorId;
        statistics = new Statistics();
        LOGGER.info("OpcTagReadingExecutor: created with a opcTagRangeSize {} between {} and {} with " +
                        "maxSignalsPerMinutePerNode {} and generatorId {}", opcTagRangeSize, opcTagRangeMin,
                opcTagRangeMax, maxSignalsPerMinutePerNode, generatorId);
    }

    public void execute(final Long durationInMinutes, final String kafkaServerBootstrap, final String kafkaTopics) {
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
                LOGGER.info("OpcTagReadingExecutor: Total Success: {}", statistics.getTotalSuccessCounts());
                LOGGER.info("OpcTagReadingExecutor: Total Errors: {}", statistics.getTotalErrorCounts());
            } catch (final Throwable ignore) {
            }
            try {
                forkJoinPool.shutdownNow();
            } catch (final Throwable ignore) {
            }
        }
    }

    private void dispatchPerOpcTagReading(final String kafkaServerBootstrap, final String kafkaTopics,
                                          final String opcTagId, final Long durationInMinutes, final CountDownLatch latch) {
        LOGGER.debug("OpcTagReadingExecutor: Dispatching signals for {}", opcTagId);

        try (final KafkaProducer<String, byte[]> kafkaProducer = getKafkaProducer(kafkaServerBootstrap)) {
            // generate signals for the specific amount of time in minutes for a specific opc tag reading
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown((int) TimeUnit.MINUTES.toMinutes(durationInMinutes));

            final RateLimiter rateLimiter = RateLimiter.create(signalsPerSecond(maxSignalsPerMinutePerNode));
            final Random randomNumberGenerator = new Random();

            while (true) {
                // dispatchSingleSignal until duration ends at a rate specified by max signal
                dispatchSingleSignal(kafkaProducer, kafkaTopics, opcTagId, rateLimiter, randomNumberGenerator);

                if (countdownTimer.isFinished()) {
                    LOGGER.debug("OpcTagReadingExecutor: Completed dispatching signals for {} ", opcTagId);
                    LOGGER.debug("\tOpcTagReadingExecutor: {} : Success {}", opcTagId, statistics.getTotalSuccessCountsById(opcTagId));
                    LOGGER.debug("\tOpcTagReadingExecutor: {} : Error {}", opcTagId, statistics.getTotalErrorCountsById(opcTagId));
                    break;
                }
            }
        } finally {
            latch.countDown();
        }
    }

    private void dispatchSingleSignal(final KafkaProducer<String, byte[]> kafkaProducer, final String kafkaTopics,
                                      final String opcTagId, final RateLimiter rateLimiter,
                                      final Random randomNumberGenerator) {
        rateLimiter.acquire();
        // signals will just be created from json string provided by grainger and updated OPCTagID, Timestamp, Value
        final String timestamp = DateTime.now().toDateTimeISO().toString(); // 2016-03-03T19:13:13.3980463Z
        final double value = randomNumberGenerator.nextGaussian();
        final String signal = String.format(" [{\n" +
                " \"ExtendedProperties\":[%s],\n" +
                " \"ProxiedTypeName\":\"Odatech.Business.Integration.OPCTagReading\",\n" +
                " \"OPCTagID\":%s,\n" +
                " \"OPCTagReadingID\":0,\n" +
                " \"Quality\":192,\n" +
                " \"Timestamp\":\"%s\",\n" +
                " \"Value\":\"%s\",\n" +
                " \"ID\":0,\n" +
                " \"UniqueID\":null\n" +
                " }]", generatorId, opcTagId, timestamp, value);

        // for now signal time and value are just randomly genetated
        try {
            final Future<RecordMetadata> send =
                    kafkaProducer.send(new ProducerRecord<>(kafkaTopics, signal.getBytes(Charset.defaultCharset())));
            // get will wait until a response
            final RecordMetadata recordMetadata = send.get();
            LOGGER.debug("OpcTagReadingExecutor: {} successfully sent >{}< to {}",opcTagId, signal,
                    recordMetadata.topic());
            // increment counts
            if (send.isDone() && !send.isCancelled()) {
                statistics.incrementSuccessCountById(opcTagId);
            } else {
                statistics.incrementErrorCountById(opcTagId);
            }
        } catch (final Throwable any) {
            statistics.incrementErrorCountById(opcTagId);
            LOGGER.error(String.format("OpcTagReadingExecutor: Unexpected error: dispatching signal to >%s<",
                    kafkaTopics), any);
        }
    }

    private static double signalsPerSecond(final int signalsPerMinutes) {
        return (double) Math.round((signalsPerMinutes / 60d) * 100) / 100;
    }

    public static void main(String[] args) {
        if (args == null || args.length < 7) {
            // 100 110 30 3 10.40.217.211:9092 mm_signals test
            throw new IllegalArgumentException("OpcTagReadingExecutor: Please ensure the following are set: " +
                    "opcTagRangeMin, opcTagRangeMax, maxSignalsPerMinutePerNode, durationInMinutes," +
                    " kafkaServerBootstrap, kafkaTopics, and generatorId");
        }
        final int opcTagRangeMin = Integer.valueOf(args[0]);
        final int opcTagRangeMax = Integer.valueOf(args[1]);
        final int maxSignalsPerMinutePerNode = Integer.valueOf(args[2]);
        final long durationInMinutes = Long.valueOf(args[3]);
        final String kafkaServerBootstrap = args[4];
        final String kafkaTopics = args[5];
        final String generatorId = args[6];

        try {
            final OpcTagReadingExecutor opcTagReadingExecutor = new OpcTagReadingExecutor(opcTagRangeMin,
                    opcTagRangeMax, maxSignalsPerMinutePerNode, generatorId);
            opcTagReadingExecutor.execute(durationInMinutes, kafkaServerBootstrap, kafkaTopics);
        } finally {
            Runtime.getRuntime().halt(0);
        }
    }
}
