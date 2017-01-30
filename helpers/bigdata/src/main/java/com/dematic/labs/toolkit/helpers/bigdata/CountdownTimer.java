package com.dematic.labs.toolkit.helpers.bigdata;

import com.dematic.labs.toolkit.helpers.bigdata.communication.EventUtils;
import org.joda.time.DateTime;

import java.util.Timer;
import java.util.TimerTask;

public final class CountdownTimer {
    private boolean finished;

    /**
     * Count down from time called in minutes.
     *
     * @param inMinutes -- number of minutes to count down
     */
    public void countDown(final int inMinutes) {
        final DateTime nowPlusMinutes = DateTime.now().plusMinutes(inMinutes);
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (EventUtils.now().isAfter(nowPlusMinutes)) {
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
