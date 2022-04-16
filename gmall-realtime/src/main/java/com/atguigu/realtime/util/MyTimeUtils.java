package com.atguigu.realtime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/16 11:31
 */
public class MyTimeUtils {
    public static Long toTs(String dateTime,String format){
        try {
            return new SimpleDateFormat(format).parse(dateTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
