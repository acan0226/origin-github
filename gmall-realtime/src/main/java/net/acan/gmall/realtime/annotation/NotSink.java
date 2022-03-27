package net.acan.gmall.realtime.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// 注解的使用范围: 这个注解用在哪
@Target(ElementType.FIELD)
// 注解的生命周期
@Retention(RetentionPolicy.RUNTIME)
public @interface NotSink {
}
