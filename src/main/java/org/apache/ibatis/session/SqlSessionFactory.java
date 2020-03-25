/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.session;

import java.sql.Connection;

/**
 * Creates an {@link SqlSession} out of a connection or a DataSource
 *
 * 1. 使用xml来构建 SQLSessionFactory
 * String resource = "org/mybatis/example/mybatis-config.xml";
 * InputStream inputStream = Resources.getResourceAsStream(resource);
 * SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
 *
 * 2.不使用 XML 构建 SqlSessionFactory
 * DataSource dataSource = BlogDataSourceFactory.getBlogDataSource();
 * TransactionFactory transactionFactory = new JdbcTransactionFactory();
 * Environment environment = new Environment("development", transactionFactory, dataSource);
 * Configuration configuration = new Configuration(environment);
 * configuration.addMapper(BlogMapper.class);
 * SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
 *
 * 3. SqlSessionFactory 一旦被创建就应该在应用的运行期间一直存在，没有任何理由丢弃它或重新创建另一个实例。
 * 使用 SqlSessionFactory 的最佳实践是在应用运行期间不要重复创建多次，
 * 多次重建 SqlSessionFactory 被视为一种代码“坏习惯”。因此 SqlSessionFactory 的最佳作用域是应用作用域。
 * 有很多方法可以做到，最简单的就是使用单例模式或者静态单例模式。
 *
 * @author Clinton Begin
 */
public interface SqlSessionFactory {

  SqlSession openSession();

  SqlSession openSession(boolean autoCommit);

  SqlSession openSession(Connection connection);

  SqlSession openSession(TransactionIsolationLevel level);

  SqlSession openSession(ExecutorType execType);

  SqlSession openSession(ExecutorType execType, boolean autoCommit);

  SqlSession openSession(ExecutorType execType, TransactionIsolationLevel level);

  SqlSession openSession(ExecutorType execType, Connection connection);

  Configuration getConfiguration();

}
