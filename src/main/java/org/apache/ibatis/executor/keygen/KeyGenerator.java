/**
 * Copyright 2009-2015 the original author or authors.
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
package org.apache.ibatis.executor.keygen;

import java.sql.Statement;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;

/**
 * 在 SQL 执行之前或之后，进行处理主键的生成。
 *
 * <pre>
 *     @Options(useGeneratedKeys = true, keyProperty = "id")
 *     @Insert({"insert into country (countryname,countrycode) values (#{countryname},#{countrycode})"})
 *     int insertBean(Country country);
 * </pre>
 * KeyGenerator 在获取到主键后，会设置回 parameter 参数的对应属性。
 *
 * @author Clinton Begin
 */
public interface KeyGenerator {

  /**
   * SQL 执行前
   *
   * @param executor  SQL执行器
   * @param ms        每个SQL对应一个 MappedStatement
   * @param stmt      SQL执行Statement
   * @param parameter 参数
   */
  void processBefore(Executor executor, MappedStatement ms, Statement stmt, Object parameter);

  /**
   * SQL 执行后
   *
   * @param executor  SQL执行器
   * @param ms        每个SQL对应一个 MappedStatement
   * @param stmt      SQL执行Statement
   * @param parameter 参数
   */
  void processAfter(Executor executor, MappedStatement ms, Statement stmt, Object parameter);

}
