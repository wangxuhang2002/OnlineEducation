package com.atguigu.online.education.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;


public class DateFormatUtil {
    //获取 当前日期 的 整数形式
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
