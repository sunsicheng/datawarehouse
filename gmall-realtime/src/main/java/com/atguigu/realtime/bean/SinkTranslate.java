package com.atguigu.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/5/3 13:29
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME )
public @interface SinkTranslate {
}
