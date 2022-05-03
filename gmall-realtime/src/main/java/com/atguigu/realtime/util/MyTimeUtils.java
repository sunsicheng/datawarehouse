package com.atguigu.realtime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/16 11:31
 */
public class MyTimeUtils {
    public static Long toTs(String dateTime, String... format) {
        try {
            String f = "yyyy-MM-dd HH:mm:ss";
            if (format.length == 1) {
                f = format[0];
            }
            return new SimpleDateFormat(f).parse(dateTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String toDateTimeString(Long dateTime, String... format) {
        try {
            String f = "yyyy-MM-dd HH:mm:ss";
            if (format.length == 1) {
                f = format[0];
            }
            return new SimpleDateFormat(f).format(new Date(dateTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
