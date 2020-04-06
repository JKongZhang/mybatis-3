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
package org.apache.ibatis.scripting.xmltags;

import org.apache.ibatis.builder.SqlSourceBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;

/**
 * 实现 SqlSource 接口，动态的 SqlSource 实现类。
 * <p>
 * 适用于使用了 OGNL 表达式，或者使用了 ${} 表达式的 SQL ，
 * 所以它是动态的，需要在每次执行 #getBoundSql(Object parameterObject) 方法，根据参数，生成对应的 SQL 。
 *
 * @author Clinton Begin
 */
public class DynamicSqlSource implements SqlSource {

  private final Configuration configuration;
  /**
   * 根 SqlNode 对象
   */
  private final SqlNode rootSqlNode;

  public DynamicSqlSource(Configuration configuration, SqlNode rootSqlNode) {
    this.configuration = configuration;
    this.rootSqlNode = rootSqlNode;
  }

  @Override
  public BoundSql getBoundSql(Object parameterObject) {
    // 1. 创建DynamicContext，并应用 rootSqlNode，相当于生成动态 SQL 。
    DynamicContext context = new DynamicContext(configuration, parameterObject);
    rootSqlNode.apply(context);
    // 2. 创建 SqlSourceBuilder 对象
    SqlSourceBuilder sqlSourceParser = new SqlSourceBuilder(configuration);
    // 3. 解析出 SqlSource 对象
    Class<?> parameterType = parameterObject == null ? Object.class : parameterObject.getClass();
    // 将占位符“#{}”，替换为“?”，并获取该占位符对应的 ParameterMapping 对象。
    // 同时创建SqlSource对象，类型是 StaticSqlSource 类。
    SqlSource sqlSource = sqlSourceParser.parse(context.getSql(), parameterType, context.getBindings());
    // 4. 获得 BoundSql 对象
    BoundSql boundSql = sqlSource.getBoundSql(parameterObject);
    // 5. 添加附加参数到 BoundSql 对象中
    context.getBindings().forEach(boundSql::setAdditionalParameter);
    // 6. 返回 BoundSql 对象return boundSql;
    return boundSql;
  }

}
