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
package org.apache.ibatis.scripting.defaults;

import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;

/**
 * As of 3.2.4 the default XML language is able to identify static statements
 * and create a {@link RawSqlSource}. So there is no need to use RAW unless you
 * want to make sure that there is not any dynamic tag for any reason.
 * <p>
 * 继承 XMLLanguageDriver 类，RawSqlSource 语言驱动器实现类，确保创建的 SqlSource 是 RawSqlSource 类。
 * <p>
 * 这是一个简单的语言驱动，只能针对静态SQL的处里，如果出现动态SQL标签会抛出异常.
 * 默认XML语言是能够识别静态语句，并创建一个{@link RawSqlSource}，所以没有需要使用原始的，除非你想确保没有任何动态标记任何理由。
 *
 * @author Eduardo Macarron
 * @since 3.2.0
 */
public class RawLanguageDriver extends XMLLanguageDriver {

  /**
   * 创建一个{@link SqlSource} 读取映射XML文件
   *
   * @param configuration The MyBatis configuration
   * @param script        XNode parsed from a XML file
   * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be null.
   *                      输入参数类型从一个xml类型映射器参数中指定的方法或属性。可以为空。
   * @return SqlSource
   */
  @Override
  public SqlSource createSqlSource(Configuration configuration, XNode script, Class<?> parameterType) {
    // 调用父类，创建 SqlSource 对象
    SqlSource source = super.createSqlSource(configuration, script, parameterType);
    // 校验创建的是 RawSqlSource 对象
    checkIsNotDynamic(source);
    return source;
  }

  /**
   * 创建 SqlSource 对象，从方法注解配置，即 @Select 等。
   *
   * @param configuration The MyBatis configuration
   * @param script        The content of the annotation
   * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be null.
   *                      方法参数，可以为空。
   * @return SqlSource
   */
  @Override
  public SqlSource createSqlSource(Configuration configuration, String script, Class<?> parameterType) {
    // 调用父类，创建 SqlSource 对象
    SqlSource source = super.createSqlSource(configuration, script, parameterType);
    // 校验创建的是 RawSqlSource 对象
    checkIsNotDynamic(source);
    return source;
  }

  /**
   * 校验是 RawSqlSource 对象
   *
   * @param source 创建的 SqlSource 对象
   */
  private void checkIsNotDynamic(SqlSource source) {
    if (!RawSqlSource.class.equals(source.getClass())) {
      throw new BuilderException("Dynamic content is not allowed when using RAW language");
    }
  }

}
