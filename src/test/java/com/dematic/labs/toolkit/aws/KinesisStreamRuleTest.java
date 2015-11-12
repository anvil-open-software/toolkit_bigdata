package com.dematic.labs.toolkit.aws;

import com.dematic.labs.toolkit.SystemPropertyRule;
import com.jayway.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.concurrent.TimeUnit;

public final class KinesisStreamRuleTest {
    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule);
    // note: the before/after will create and destroy the kinesis streams

    @Test
    public void pushEventsUntilTimeAllocation(){
        Awaitility.await().atMost(2, TimeUnit.MINUTES).until(() -> {
            kinesisStreamRule.pushEventsToKinesis(100, 1, TimeUnit.MINUTES);
        });
    }
}