/*
 *  Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.toolkit_bigdata.simulators.grainger;

import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer;
import com.dematic.labs.toolkit_bigdata.simulators.configuration.grainger.OpcTagReaderConfiguration;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class OpcTagReadingExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpcTagReadingExecutor.class);

    private final OpcTagReaderConfiguration config;

    private OpcTagReadingExecutor(final OpcTagReaderConfiguration config) {
        this.config = config;
    }

    private void execute() {
        final int opcTagRangeSize = config.getOpcTagRangeMax() - config.getOpcTagRangeMin();
        final CountDownLatch latch = new CountDownLatch(opcTagRangeSize);
        final ForkJoinPool forkJoinPool =
                new ForkJoinPool(opcTagRangeSize, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null,
                        true);
        try {
            final Stream<String> opcTagRangeIds =
                    IntStream.range(config.getOpcTagRangeMin(), config.getOpcTagRangeMax()).mapToObj(String::valueOf);
            opcTagRangeIds.forEach(opcTagId -> forkJoinPool.submit(() ->
                    dispatchPerOpcTagReading(opcTagId, latch)));
            // wait 5 minutes longer then duration
            latch.await(config.getDurationInMinutes() + 5, TimeUnit.MINUTES);
        } catch (final Throwable any) {
            LOGGER.error("OpcTagReadingExecutor: Unhandled Error: stopping execution", any);
        } finally {
            try {
                forkJoinPool.shutdownNow();
            } catch (final Throwable ignore) {
            }
        }
    }

    private void dispatchPerOpcTagReading(final String opcTagId, final CountDownLatch latch) {
        LOGGER.debug("OpcTagReadingExecutor: Dispatching signals for {}", opcTagId);
        // create kafka configuration
        final Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializer());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializer());
        properties.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        properties.put(ProducerConfig.RETRIES_CONFIG, config.getRetries());

        try (final KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<>(properties)) {
            // generate signals for the specific amount of time in minutes for a specific opc tag reading
            final CountdownTimer countdownTimer = new CountdownTimer();
            countdownTimer.countDown((int) TimeUnit.MINUTES.toMinutes(config.getDurationInMinutes()));

            final RateLimiter rateLimiter =
                    RateLimiter.create(signalsPerSecond(config.getMaxSignalsPerMinutePerOpcTag()));
            final Random randomNumberGenerator = new Random();

            while (true) {
                // dispatchSingleSignal until duration ends at a rate specified by max signal
                dispatchSingleSignal(kafkaProducer, config.getTopics(), opcTagId, rateLimiter, randomNumberGenerator);

                if (countdownTimer.isFinished()) {
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
                " }]", config.getId(), opcTagId, timestamp, value);

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
        try {
            // configuration comes from the application.conf for the driver
            final OpcTagReaderConfiguration config = new OpcTagReaderConfiguration.Builder().build();
            new OpcTagReadingExecutor(config).execute();
        } catch (final Throwable any) {
            LOGGER.error("Producer failed to startup:", any);
        } finally {
            Runtime.getRuntime().halt(0);
        }
    }
}
