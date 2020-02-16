package com.oppo.tagbase.job.util;

import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by daikai on 2020/2/16.
 */
public final class IdGenerator {
    // a-z, 0-9, except: l, o, 0, 1
    private static final char[] BASE_32 = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '2', '3', '4', '5', '6', '7', '8', '9'};

    private static final String DATE_FORMATS = "yyyyMMdd_HHmmss";

    public static final IdGenerator INSTANCE = new IdGenerator();

    private String id;
    private int counter;
    private long lastTimeInDays;
    private long lastTimeInSeconds;

    public IdGenerator() {
        StringBuilder idBuilder = new StringBuilder(5);
        for (int i = 0; i < 5; i++) {
            idBuilder.append(BASE_32[ThreadLocalRandom.current().nextInt(32)]);
        }
        id = idBuilder.toString();
    }


    public synchronized String nextQueryId(String start) {
        // only generate 1,000,000 ids per day
        if (counter > 999_999) {
            counter = 0;
        }

        // if it has been a second since the last id was generated, generate a new timestamp
        long now = nowInMillis();

        if (MILLISECONDS.toSeconds(now) != lastTimeInSeconds) {
            // generate new timestamp
            lastTimeInSeconds = MILLISECONDS.toSeconds(now);

            // if the day has rolled over, restart the counter
            if (MILLISECONDS.toDays(now) != lastTimeInDays) {
                lastTimeInDays = MILLISECONDS.toDays(now);
                counter = 0;
            }
        }

        String lastTimestamp = new SimpleDateFormat(DATE_FORMATS).format(now);
        return String.format("%s_%s_%06d_%s", start, lastTimestamp, counter++, id);
    }

    public synchronized String nextQueryId(String start, String date) {
        // if it has been a second since the last id was generated, generate a new timestamp
        long now = nowInMillis();

        String lastDate = new SimpleDateFormat(date).format(now);
        return String.format("%s_%s", start, lastDate);
    }

    private long nowInMillis(){
        return System.currentTimeMillis();
    }
}
