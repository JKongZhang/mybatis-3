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
package org.apache.ibatis.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.UnknownTypeHandler;

/**
 * The annotation that specify a mapping definition for the property.
 *
 * @author Clinton Begin
 * @see Results
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(Results.class)
public @interface Result {
  /**
   * Returns whether id column or not.
   * 是否是 ID 字段
   *
   * @return {@code true} if id column; {@code false} if otherwise
   */
  boolean id() default false;

  /**
   * Return the column name(or column label) to map to this argument.
   * 类中的属性
   *
   * @return the column name(or column label)
   */
  String column() default "";

  /**
   * Returns the property name for applying this mapping.
   * 数据库的字段
   *
   * @return the property name
   */
  String property() default "";

  /**
   * Return the java type for this argument.
   * Java Type
   *
   * @return the java type
   */
  Class<?> javaType() default void.class;

  /**
   * Return the jdbc type for column that map to this argument.
   * JDBC Type
   *
   * @return the jdbc type
   */
  JdbcType jdbcType() default JdbcType.UNDEFINED;

  /**
   * Returns the {@link TypeHandler} type for retrieving a column value from result set.
   * 使用的 TypeHandler 处理器
   *
   * @return the {@link TypeHandler} type
   */
  Class<? extends TypeHandler> typeHandler() default UnknownTypeHandler.class;

  /**
   * Returns the mapping definition for single relationship.
   * {@link One} 注解
   *
   * @return the mapping definition for single relationship
   */
  One one() default @One;

  /**
   * Returns the mapping definition for collection relationship.
   * {@link Many} 注解
   *
   * @return the mapping definition for collection relationship
   */
  Many many() default @Many;
}
