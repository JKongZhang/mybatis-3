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
package org.apache.ibatis.session;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;

/**
 * Builds {@link SqlSession} instances.
 * todo 项目入口
 * <p>
 * 这个类可以被实例化、使用和丢弃，一旦创建了 SqlSessionFactory，就不再需要它了。
 * 因此 SqlSessionFactoryBuilder 实例的最佳作用域是方法作用域（也就是局部方法变量）。
 * 你可以重用 SqlSessionFactoryBuilder 来创建多个 SqlSessionFactory 实例，
 * 但最好还是不要一直保留着它，以保证所有的 XML 解析资源可以被释放给更重要的事情。
 * <p>
 * 在 MyBatis 初始化过程中，会加载 mybatis-config.xml 配置文件、映射配置文件以及 Mapper 接口中的注解信息，
 * 解析后的配置信息会形成相应的对象并保存到 Configuration 对象中。
 * - <resultMap>节点(即 ResultSet 的映射规则) 会被解析成 ResultMap 对象。
 * - <result> 节点(即属性映射)会被解析成 ResultMapping 对象。
 *
 * @author Clinton Begin
 */
public class SqlSessionFactoryBuilder {

  public SqlSessionFactory build(Reader reader) {
    return build(reader, null, null);
  }

  public SqlSessionFactory build(Reader reader, String environment) {
    return build(reader, environment, null);
  }

  public SqlSessionFactory build(Reader reader, Properties properties) {
    return build(reader, null, properties);
  }

  /**
   * 创建SQLSessionFactory工厂：
   * 1. 创建 XMLConfigBuilder 对象
   * 2. 调用 XMLConfigBuilder#parse() 方法，执行 XML 解析，返回 Configuration 对象。
   * 3. 创建 DefaultSqlSessionFactory 对象
   *
   * @param reader      reader读取文件
   * @param environment 配置的环境（比如：开发，测试，生产）
   * @param properties  配置文件信息
   * @return SqlSessionFactory
   */
  public SqlSessionFactory build(Reader reader, String environment, Properties properties) {
    try {
      // 1. 创建 XMLConfigBuilder 对象
      XMLConfigBuilder parser = new XMLConfigBuilder(reader, environment, properties);
      // 2. 调用 XMLConfigBuilder#parse() 方法，执行 XML 解析，返回 Configuration 对象。
      // 3. 创建 DefaultSqlSessionFactory 对象
      return build(parser.parse());
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error building SqlSession.", e);
    } finally {
      ErrorContext.instance().reset();
      try {
        reader.close();
      } catch (IOException e) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }

  public SqlSessionFactory build(InputStream inputStream) {
    return build(inputStream, null, null);
  }

  public SqlSessionFactory build(InputStream inputStream, String environment) {
    return build(inputStream, environment, null);
  }

  public SqlSessionFactory build(InputStream inputStream, Properties properties) {
    return build(inputStream, null, properties);
  }

  public SqlSessionFactory build(InputStream inputStream, String environment, Properties properties) {
    try {
      XMLConfigBuilder parser = new XMLConfigBuilder(inputStream, environment, properties);
      return build(parser.parse());
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error building SqlSession.", e);
    } finally {
      ErrorContext.instance().reset();
      try {
        inputStream.close();
      } catch (IOException e) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }

  /**
   * 创建SQLSessionFactory的默认实现类{@link DefaultSqlSessionFactory}对象
   *
   * @param config mybatis 核心配置
   * @return SQLSessionFactory对象
   */
  public SqlSessionFactory build(Configuration config) {
    return new DefaultSqlSessionFactory(config);
  }

}
