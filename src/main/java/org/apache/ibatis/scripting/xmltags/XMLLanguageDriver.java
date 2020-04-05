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
package org.apache.ibatis.scripting.xmltags;

import org.apache.ibatis.builder.xml.XMLMapperEntityResolver;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;

/**
 * 实现 LanguageDriver 接口，XML 语言驱动实现类。
 * <p>
 * Mybatis默认XML驱动类为XMLLanguageDriver，其主要作用于解析select|update|insert|delete节点为完整的SQL语句。
 * </p>
 *
 * @author Eduardo Macarron
 */
public class XMLLanguageDriver implements LanguageDriver {

  @Override
  public ParameterHandler createParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql) {
    // 创建 DefaultParameterHandler 对象
    return new DefaultParameterHandler(mappedStatement, parameterObject, boundSql);
  }

  /**
   * 创建一个{@link SqlSource} 读取映射XML文件
   *
   * @param configuration The MyBatis configuration
   * @param script        XNode parsed from a XML file
   * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be null.
   *                      输入参数类型从一个xml类型映射器参数中指定的方法或属性。可以为空。
   * @return
   */
  @Override
  public SqlSource createSqlSource(Configuration configuration, XNode script, Class<?> parameterType) {
    // 创建 XMLScriptBuilder 对象，执行解析
    XMLScriptBuilder builder = new XMLScriptBuilder(configuration, script, parameterType);
    /*
     *  当sql中包含有${}时，就认为是动态SQL
     *  或者 当DML标签存在动态SQL标签名，如<if> <trim>等，就认为是动态SQL
     *  ，如果是动态返回 {@link DynamicSqlSource} ，否则 {@link RawSqlSource}
     */
    return builder.parseScriptNode();
  }

  /**
   * 创建 SqlSource 对象，从方法注解配置，即 @Select 等。
   * <pre>
   *  @Update(value = {
   *    "<script>",
   *        "update Author",
   *        "  <set>",
   *        "    <if test='username != null'>username=#{username},</if>",
   *        "    <if test='password != null'>password=#{password},</if>",
   *        "    <if test='email != null'>email=#{email},</if>",
   *        "    <if test='bio != null'>bio=#{bio}</if>",
   *        "  </set>",
   *        "where id=#{id}",
   *    "</script>"})
   *  void updateAuthorValues(Author author);
   * </pre>
   *
   * @param configuration The MyBatis configuration
   * @param script        The content of the annotation
   * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be null.
   *                      方法参数，可以为空。
   * @return SqlSource
   */
  @Override
  public SqlSource createSqlSource(Configuration configuration, String script, Class<?> parameterType) {
    // issue #3
    // 如果是 <script> 开头，使用 XML 配置的方式，使用动态 SQL
    if (script.startsWith("<script>")) {
      // 创建 XPathParser 对象，用来解析出 <script /> 节点
      XPathParser parser = new XPathParser(script, false, configuration.getVariables(), new XMLMapperEntityResolver());
      // 调用上面的 #createSqlSource(...) 方法，创建 SqlSource 对象
      return createSqlSource(configuration, parser.evalNode("/script"), parameterType);
    } else {
      // issue #127
      /*
       * 解析Configuration#variable变量,将有${...}形式的字符串转换成对应字符串,
       *    eg:  '${first_name},${initial},${last_name}' => 'James,T,Kirk'
       */
      // 变量替换
      script = PropertyParser.parse(script, configuration.getVariables());
      // 创建 TextSqlNode 对象
      TextSqlNode textSqlNode = new TextSqlNode(script);
      // 根据TextSqlNode的内部属性isDynamic来进行解析帮助类的分配，如果是动态 SQL ，则创建 DynamicSqlSource 对象
      if (textSqlNode.isDynamic()) {
        return new DynamicSqlSource(configuration, textSqlNode);
        // 如果非动态 SQL ，则创建 RawSqlSource 对象
      } else {
        return new RawSqlSource(configuration, script, parameterType);
      }
    }
  }

}
