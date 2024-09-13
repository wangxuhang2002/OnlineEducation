package com.atguigu.online.education.common.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateFormatUtil {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final DateTimeFormatter dtfForPartition = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 毫秒事件戳 转 年月日,如： 1999-01-01
    public static String tsToDate(Long ts){
        Date date = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    // 毫秒事件戳 转 年月日时分秒,如： 1999-01-01 01:01:01
    public static String tsToDateTime(Long ts){
        Date date = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    // 毫秒事件戳 转 年月日,如： 19990101
    public static String tsToDateForPartition(Long ts){
        Date date = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtfForPartition.format(localDateTime);
    }

    // 年月日时分秒 转 毫秒事件戳，如：0000000000000
    public static Long dateTimeToTs(String dateTime){
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, dtfFull);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    // 年月日 转 毫秒事件戳，如：0000000000000
    public static Long dateToTs(String date){
        return dateTimeToTs(date + " 00:00:00");
    }
}
