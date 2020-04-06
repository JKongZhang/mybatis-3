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
package org.apache.ibatis.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.parsing.TokenHandler;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;

/**
 * 继承 BaseBuilder 抽象类，SqlSource 构建器，
 * 负责将 SQL 语句中的 #{} 替换成相应的 ? 占位符，
 * 并获取该 ? 占位符对应的 org.apache.ibatis.mapping.ParameterMapping 对象。
 *
 * @author Clinton Begin
 */
public class SqlSourceBuilder extends BaseBuilder {

  /**
   * <pre>
   *    <insert id="insertUser" parameterType="User">
   *      insert into users (id, username, age)
   *      values (#{id}, #{username}, #{age})
   *    </insert>
   *
   *    要更进一步地自定义类型处理方式，可以指定一个特殊的类型处理器类（或别名），比如，对#{age}进行处理：
   *    #{age,javaType=int,jdbcType=integer,typeHandler=MyTypeHandler}
   *    尽管上面这些选项很强大，但大多时候，你只须简单指定属性名，顶多要为可能为空的列指定 jdbcType，其他的事情交给 MyBatis 自己去推断就行了。
   *    #{age,jdbcType=integer}
   * </pre>
   * https://mybatis.org/mybatis-3/zh/sqlmap-xml.html#Parameters
   */
  private static final String PARAMETER_PROPERTIES = "javaType,jdbcType,mode,numericScale,resultMap,typeHandler,jdbcTypeName";

  public SqlSourceBuilder(Configuration configuration) {
    super(configuration);
  }

  /**
   * 执行解析原始 SQL ，成为 SqlSource 对象
   *
   * @param originalSql          原始 SQL
   * @param parameterType        参数类型
   * @param additionalParameters 附加参数集合。
   *                             可能是空集合，也可能是 {@link org.apache.ibatis.scripting.xmltags.DynamicContext#bindings} 集合
   * @return SqlSource 对象
   * <p>
   * 感叹：设计模式真的是随手拈来。
   */
  public SqlSource parse(String originalSql,
                         Class<?> parameterType,
                         Map<String, Object> additionalParameters) {
    // 1. 创建 ParameterMappingTokenHandler 对象
    ParameterMappingTokenHandler handler = new ParameterMappingTokenHandler(configuration, parameterType, additionalParameters);
    // 2. 创建 GenericTokenParser 对象，并指定左右token符号，作为站位符查找边界。
    GenericTokenParser parser = new GenericTokenParser("#{", "}", handler);
    // 3. 执行解析，并会调用 ParameterMappingTokenHandler 的 handleToken 方法
    String sql = parser.parse(originalSql);
    // 4. 创建 StaticSqlSource 对象
    return new StaticSqlSource(configuration, sql, handler.getParameterMappings());
  }

  /**
   * 实现 TokenHandler 接口，继承 BaseBuilder 抽象类，
   * 负责将匹配到的 #{ 和 } 对，替换成相应的 ? 占位符，
   * 并获取该 ? 占位符对应的 org.apache.ibatis.mapping.ParameterMapping 对象。
   */
  private static class ParameterMappingTokenHandler extends BaseBuilder implements TokenHandler {

    /**
     * ParameterMapping 数组
     */
    private List<ParameterMapping> parameterMappings = new ArrayList<>();
    /**
     * 参数类型
     */
    private Class<?> parameterType;
    /**
     * additionalParameters 参数的对应的 MetaObject 对象
     */
    private MetaObject metaParameters;

    public ParameterMappingTokenHandler(Configuration configuration, Class<?> parameterType, Map<String, Object> additionalParameters) {
      super(configuration);
      this.parameterType = parameterType;
      // 创建 additionalParameters 参数的对应的 MetaObject 对象
      this.metaParameters = configuration.newMetaObject(additionalParameters);
    }

    public List<ParameterMapping> getParameterMappings() {
      return parameterMappings;
    }

    @Override
    public String handleToken(String content) {
      // <1> 构建 ParameterMapping 对象，并添加到 parameterMappings 中
      parameterMappings.add(buildParameterMapping(content));
      // <2> 返回 ? 占位符
      return "?";
    }

    /**
     * 构建 ParameterMapping 对象。
     *
     * @param content {@link GenericTokenParser#parse(String)} 处理后的 expression
     * @return ParameterMapping
     */
    private ParameterMapping buildParameterMapping(String content) {
      Map<String, String> propertiesMap = parseParameterMapping(content);
      String property = propertiesMap.get("property");
      Class<?> propertyType;
      if (metaParameters.hasGetter(property)) { // issue #448 get type from additional params
        propertyType = metaParameters.getGetterType(property);
      } else if (typeHandlerRegistry.hasTypeHandler(parameterType)) {
        propertyType = parameterType;
      } else if (JdbcType.CURSOR.name().equals(propertiesMap.get("jdbcType"))) {
        propertyType = java.sql.ResultSet.class;
      } else if (property == null || Map.class.isAssignableFrom(parameterType)) {
        propertyType = Object.class;
      } else {
        MetaClass metaClass = MetaClass.forClass(parameterType, configuration.getReflectorFactory());
        if (metaClass.hasGetter(property)) {
          propertyType = metaClass.getGetterType(property);
        } else {
          propertyType = Object.class;
        }
      }
      ParameterMapping.Builder builder = new ParameterMapping.Builder(configuration, property, propertyType);
      Class<?> javaType = propertyType;
      String typeHandlerAlias = null;
      for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        if ("javaType".equals(name)) {
          javaType = resolveClass(value);
          builder.javaType(javaType);
        } else if ("jdbcType".equals(name)) {
          builder.jdbcType(resolveJdbcType(value));
        } else if ("mode".equals(name)) {
          builder.mode(resolveParameterMode(value));
        } else if ("numericScale".equals(name)) {
          builder.numericScale(Integer.valueOf(value));
        } else if ("resultMap".equals(name)) {
          builder.resultMapId(value);
        } else if ("typeHandler".equals(name)) {
          typeHandlerAlias = value;
        } else if ("jdbcTypeName".equals(name)) {
          builder.jdbcTypeName(value);
        } else if ("property".equals(name)) {
          // Do Nothing
        } else if ("expression".equals(name)) {
          throw new BuilderException("Expression based parameters are not supported yet");
        } else {
          throw new BuilderException("An invalid property '" + name + "' was found in mapping #{" + content + "}.  Valid properties are " + PARAMETER_PROPERTIES);
        }
      }
      if (typeHandlerAlias != null) {
        builder.typeHandler(resolveTypeHandler(javaType, typeHandlerAlias));
      }
      return builder.build();
    }

    /**
     * 假设 content = "#{age,javaType=int,jdbcType=integer,typeHandler=MyTypeHandler}"
     * 解析结果为：
     * <pre>
     *     {
     *         "property":"age",
     *         "javaType":"int",
     *         "jdbcType":"integer",
     *         "typeHandler":"MyTypeHandler"
     *     }
     * </pre>
     *
     * @param content
     * @return
     */
    private Map<String, String> parseParameterMapping(String content) {
      try {
        return new ParameterExpression(content);
      } catch (BuilderException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new BuilderException("Parsing error was found in mapping #{" + content + "}.  Check syntax #{property|(expression), var1=value1, var2=value2, ...} ", ex);
      }
    }
  }

}
