package com.dematic.labs.toolkit_bigdata.simulators;

import org.joda.time.DateTime;

import java.util.Timer;
import java.util.TimerTask;

import static org.joda.time.DateTime.now;

public final class CountdownTimer {
    private boolean finished;

    /**
     * Count down from time called in minutes.
     *
     * @param inMinutes -- number of minutes to count down
     */
    public void countDown(final int inMinutes) {
        final DateTime nowPlusMinutes = now().plusMinutes(inMinutes);
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (now().toDateTimeISO().isAfter(nowPlusMinutes)) {
                    try {
                        timer.cancel();
                    } finally {
                        finished = true;
                    }
                }
            }
        }, 0, 60 * 1000L);
    }

    public boolean isFinished() {
        return finished;
    }
}
