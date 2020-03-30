/**
 * Copyright 2009-2019 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ibatis.logging;

import org.apache.ibatis.logging.slf4j.Slf4jImpl;

import java.lang.reflect.Constructor;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public final class LogFactory {

  /**
   * Marker to be used by logging implementations that support markers.
   */
  public static final String MARKER = "MYBATIS";

  private static Constructor<? extends Log> logConstructor;


  static {
    // 逐个尝试，判断使用哪个 Log 的实现类，即初始化 logConstructor 属性
    // private static void tryImplementation(Runnable runnable) { 方法入参为Runnable，是因为此处使用了Lambda表达式。
    // 同下：
    // tryImplementation(new Runnable() {
    //   @Override
    //   public void run() {
    //     LogFactory.useSlf4jLogging();
    //   }
    // });
    // 此处虽然使用Runnable来作为方法入参，但是并未启用过个线程分别加载日志实现，而是调用：runnable.run();
    tryImplementation(LogFactory::useSlf4jLogging);
    tryImplementation(LogFactory::useCommonsLogging);
    tryImplementation(LogFactory::useLog4J2Logging);
    tryImplementation(LogFactory::useLog4JLogging);
    tryImplementation(LogFactory::useJdkLogging);
    tryImplementation(LogFactory::useNoLogging);
  }

  private LogFactory() {
    // disable construction
  }

  /**
   * 根据日志实现类的class获取日志对象
   *
   * @param aClass 根据class获取日志对象的名称
   * @return 日志对象
   */
  public static Log getLog(Class<?> aClass) {
    return getLog(aClass.getName());
  }

  /**
   * 根据日志对象的名称获取日志对象
   *
   * @param logger 名称
   * @return 日志对象
   */
  public static Log getLog(String logger) {
    try {
      return logConstructor.newInstance(logger);
    } catch (Throwable t) {
      throw new LogException("Error creating logger for logger " + logger + ".  Cause: " + t, t);
    }
  }

  /********************************************************************************
   * 创建指定类型的Log对象
   *
   * @param clazz 日志class
   */
  public static synchronized void useCustomLogging(Class<? extends Log> clazz) {
    setImplementation(clazz);
  }

  public static synchronized void useSlf4jLogging() {
    setImplementation(org.apache.ibatis.logging.slf4j.Slf4jImpl.class);
  }

  public static synchronized void useCommonsLogging() {
    setImplementation(org.apache.ibatis.logging.commons.JakartaCommonsLoggingImpl.class);
  }

  public static synchronized void useLog4JLogging() {
    setImplementation(org.apache.ibatis.logging.log4j.Log4jImpl.class);
  }

  public static synchronized void useLog4J2Logging() {
    setImplementation(org.apache.ibatis.logging.log4j2.Log4j2Impl.class);
  }

  public static synchronized void useJdkLogging() {
    setImplementation(org.apache.ibatis.logging.jdk14.Jdk14LoggingImpl.class);
  }

  public static synchronized void useStdOutLogging() {
    setImplementation(org.apache.ibatis.logging.stdout.StdOutImpl.class);
  }

  public static synchronized void useNoLogging() {
    setImplementation(org.apache.ibatis.logging.nologging.NoLoggingImpl.class);
  }

  /**
   * 初始化日志对象
   *
   * @param runnable 执行日志对象获取Runnable
   */
  private static void tryImplementation(Runnable runnable) {
    if (logConstructor == null) {
      try {
        runnable.run();
      } catch (Throwable t) {
        // ignore
      }
    }
  }

  /**
   * 日志实现类
   *
   * @param implClass 日志实现类，比如：{@link Slf4jImpl}
   */
  private static void setImplementation(Class<? extends Log> implClass) {
    try {
      // 获取只有一个string入参的构造器
      Constructor<? extends Log> candidate = implClass.getConstructor(String.class);
      // 指定构造器入参，并实例化Log对象
      Log log = candidate.newInstance(LogFactory.class.getName());
      if (log.isDebugEnabled()) {
        log.debug("Logging initialized using '" + implClass + "' adapter.");
      }
      // 将实现Log的构造器对象设置到 logConstructor
      logConstructor = candidate;
    } catch (Throwable t) {
      throw new LogException("Error setting Log implementation.  Cause: " + t, t);
    }
  }

}
