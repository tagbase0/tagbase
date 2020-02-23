package com.oppo.tagbase.job.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by daikai on 2020/2/19.
 */
public class DateFormat {
    Logger log = LoggerFactory.getLogger(DateFormat.class);

    public Date toDate(String dateStr) {
        Date date = null;
        try {
            if (dateStr.contains("-")) {
                dateStr = dateStr.replace("-", "");
            }
            if (dateStr.contains(" ")) {
                dateStr = dateStr.split(" ")[0];
            }
            SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
            date = new Date(fmt.parse(dateStr).getTime());
        } catch (ParseException e) {
            log.info("Date conversion failed  for {} !", dateStr);
        }
        return date;
    }
}
