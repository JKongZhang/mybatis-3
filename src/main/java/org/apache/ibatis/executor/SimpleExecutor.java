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
package org.apache.ibatis.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

/**
 * 每次开始读或写操作，都创建对应的 Statement 对象。
 * 执行完成后，关闭该 Statement 对象。
 *
 * @author Clinton Begin
 */
public class SimpleExecutor extends BaseExecutor {

  public SimpleExecutor(Configuration configuration, Transaction transaction) {
    super(configuration, transaction);
  }

  /**
   * 执行数据写操作
   *
   * @param ms        每个 <select />、<insert />、<update />、<delete /> 对应一个 MappedStatement 对象
   * @param parameter 参数
   * @return 受影响的数据量
   * @throws SQLException e
   */
  @Override
  public int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
    Statement stmt = null;
    try {
      Configuration configuration = ms.getConfiguration();
      // 1. 创建 StatementHandler 对象
      StatementHandler handler = configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, null);
      // 2. 初始化 StatementHandler 对象
      stmt = prepareStatement(handler, ms.getStatementLog());
      // 3. 执行 StatementHandler ，进行写操作
      return handler.update(stmt);
    } finally {
      // 4. 关闭 StatementHandler 对象
      closeStatement(stmt);
    }
  }

  /**
   * 继承自 BaseExecutor，执行数据去查询操作
   *
   * @param ms            每一个SQL标签对应一个 MappedStatement
   * @param parameter     SQL需要的参数
   * @param rowBounds
   * @param resultHandler 结果处理器
   * @param boundSql      封装的可执行的SQL
   * @param <E>           返回体泛型
   * @return 查询结果list
   * @throws SQLException e
   */
  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler,
                             BoundSql boundSql) throws SQLException {
    Statement stmt = null;
    try {
      Configuration configuration = ms.getConfiguration();
      // 1. 创建 RoutingStatementHandler 对象，并为 RoutingStatementHandler 添加 拦截器（Interceptor）
      StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, resultHandler, boundSql);
      // 2. 初始化 StatementHandler 对象
      stmt = prepareStatement(handler, ms.getStatementLog());
      // 3. 执行 StatementHandler  ，进行读操作
      return handler.query(stmt, resultHandler);
    } finally {
      // 4. 关闭 StatementHandler 对象
      closeStatement(stmt);
    }
  }

  /**
   * 执行查询，返回的结果为 Cursor 游标对象。
   *
   * @param ms        每个 <select />、<insert />、<update />、<delete /> 对应一个 MappedStatement 对象
   * @param parameter
   * @param rowBounds
   * @param boundSql
   * @param <E>
   * @return
   * @throws SQLException
   */
  @Override
  protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql) throws SQLException {
    Configuration configuration = ms.getConfiguration();
    // 创建 StatementHandler 对象
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    // 初始化 StatementHandler 对象
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    // 执行 StatementHandler  ，进行读操作
    Cursor<E> cursor = handler.queryCursor(stmt);
    // 设置 Statement ，如果执行完成，则进行自动关闭
    stmt.closeOnCompletion();
    return cursor;
  }

  /**
   * 不存在批量操作的情况，所以直接返回空数组。
   *
   * @param isRollback 是否回滚数据
   * @return
   */
  @Override
  public List<BatchResult> doFlushStatements(boolean isRollback) {
    return Collections.emptyList();
  }

  /**
   * 初始化 Statement 对象。
   * 1. 使用Connection对象创建Statement对象；
   * 2. 初始化 Statement 对象，包括 超时时间和fetchSize；
   * 3. 如果 Statement 需要参数，那么则设置参数。
   *
   * @param handler      statementHandler 对象
   * @param statementLog 用来创建Connection大力对象，便于日志输出
   * @return 完成初始化的 java.sql.Statement
   * @throws SQLException e
   */
  private Statement prepareStatement(StatementHandler handler, Log statementLog) throws SQLException {
    Statement stmt;
    // 1. 获得 Connection 对象
    Connection connection = getConnection(statementLog);
    // 2. 创建 Statement 或 PrepareStatement 对象。
    stmt = handler.prepare(connection, transaction.getTimeout());
    // 3. 设置 SQL 上的参数，例如 PrepareStatement 对象上的占位符。
    handler.parameterize(stmt);
    return stmt;
  }

}
