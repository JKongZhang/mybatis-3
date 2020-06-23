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
package org.apache.ibatis.transaction.jdbc;

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.TransactionFactory;

/**
 * Creates {@link JdbcTransaction} instances.
 * 通过数据库来控制事务
 *
 * @author Clinton Begin
 * @see JdbcTransaction
 */
public class JdbcTransactionFactory implements TransactionFactory {

  /**
   * 通过 Connection 创建 事务对象
   *
   * @param conn Existing database connection
   * @return Transaction
   */
  @Override
  public Transaction newTransaction(Connection conn) {
    return new JdbcTransaction(conn);
  }

  /**
   * 通过数据源，事务隔离级别，是否自动提交获取事务对象
   *
   * @param ds         数据源
   * @param level      Desired isolation level 事务隔离级别
   * @param autoCommit Desired autocommit 是否自动提交
   * @return Transaction
   */
  @Override
  public Transaction newTransaction(DataSource ds, TransactionIsolationLevel level, boolean autoCommit) {
    return new JdbcTransaction(ds, level, autoCommit);
  }
}
