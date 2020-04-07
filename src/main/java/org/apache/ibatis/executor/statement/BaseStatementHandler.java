/**
 * Copyright 2009-2016 the original author or authors.
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
package org.apache.ibatis.executor.statement;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 */
public abstract class BaseStatementHandler implements StatementHandler {

  protected final Configuration configuration;
  protected final ObjectFactory objectFactory;
  protected final TypeHandlerRegistry typeHandlerRegistry;
  protected final ResultSetHandler resultSetHandler;
  protected final ParameterHandler parameterHandler;

  protected final Executor executor;
  protected final MappedStatement mappedStatement;
  protected final RowBounds rowBounds;

  protected BoundSql boundSql;

  /**
   * 通过 {@link Configuration#newStatementHandler(Executor, MappedStatement, Object, RowBounds, ResultHandler, BoundSql)}
   * 创建一个 {@link RoutingStatementHandler}，在 {@link RoutingStatementHandler} 构造器中，
   * 根据 {@link MappedStatement#getStatementType()} 创建 {@link StatementHandler} 实现类。
   *
   * @param executor        SQL执行器
   * @param mappedStatement 每个 <select />、<insert />、<update />、<delete /> 对应一个 MappedStatement 对象
   * @param parameterObject 参数
   * @param rowBounds
   * @param resultHandler   结果处理器
   * @param boundSql
   */
  protected BaseStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameterObject,
                                 RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    // 获得 Configuration 对象
    this.configuration = mappedStatement.getConfiguration();
    //
    this.executor = executor;
    this.mappedStatement = mappedStatement;
    this.rowBounds = rowBounds;

    // 获得 TypeHandlerRegistry 和 ObjectFactory 对象
    this.typeHandlerRegistry = configuration.getTypeHandlerRegistry();
    this.objectFactory = configuration.getObjectFactory();

    // issue #435, get the key before calculating the statement
    // 1. 如果 boundSql 为空，一般是写类操作，例如：insert、update、delete ，则先获得自增主键，然后再创建 BoundSql 对象
    // 可以参考SimpleExecutor中增删改查操作中，创建 StatementHandler 对象时的传参情况。
    if (boundSql == null) {
      // 获得自增主键
      generateKeys(parameterObject);
      // 创建 BoundSql 对象
      boundSql = mappedStatement.getBoundSql(parameterObject);
    }

    this.boundSql = boundSql;

    // 2. 创建 ParameterHandler 对象
    this.parameterHandler = configuration.newParameterHandler(mappedStatement, parameterObject, boundSql);
    // 3. 创建 ResultSetHandler 对象
    this.resultSetHandler = configuration.newResultSetHandler(executor, mappedStatement, rowBounds, parameterHandler, resultHandler, boundSql);
  }

  @Override
  public BoundSql getBoundSql() {
    return boundSql;
  }

  @Override
  public ParameterHandler getParameterHandler() {
    return parameterHandler;
  }

  @Override
  public Statement prepare(Connection connection, Integer transactionTimeout) throws SQLException {
    ErrorContext.instance().sql(boundSql.getSql());
    Statement statement = null;
    try {
      // 1. 创建 Statement 对象
      statement = instantiateStatement(connection);
      // 2. 设置超时时间
      setStatementTimeout(statement, transactionTimeout);
      // 3. 设置 fetchSize
      setFetchSize(statement);
      return statement;
    } catch (SQLException e) {
      // 发生异常，进行关闭
      closeStatement(statement);
      throw e;
    } catch (Exception e) {
      // 发生异常，进行关闭
      throw new ExecutorException("Error preparing statement.  Cause: " + e, e);
    }
  }

  /**
   * 实例化 Statement 对象，交给子类实现
   *
   * @param connection 数据库连接对象
   * @return
   * @throws SQLException
   */
  protected abstract Statement instantiateStatement(Connection connection) throws SQLException;

  /**
   * 设置超时时间
   *
   * @param stmt               statement对象
   * @param transactionTimeout 超时时间
   * @throws SQLException
   */
  protected void setStatementTimeout(Statement stmt, Integer transactionTimeout) throws SQLException {
    // 获得 queryTimeout
    Integer queryTimeout = null;
    if (mappedStatement.getTimeout() != null) {
      queryTimeout = mappedStatement.getTimeout();
    } else if (configuration.getDefaultStatementTimeout() != null) {
      queryTimeout = configuration.getDefaultStatementTimeout();
    }
    // 设置查询超时时间
    if (queryTimeout != null) {
      stmt.setQueryTimeout(queryTimeout);
    }
    // 设置事务超时时间
    StatementUtil.applyTransactionTimeout(stmt, queryTimeout, transactionTimeout);
  }

  protected void setFetchSize(Statement stmt) throws SQLException {
    // 获得 fetchSize 。非空，则进行设置
    Integer fetchSize = mappedStatement.getFetchSize();
    if (fetchSize != null) {
      stmt.setFetchSize(fetchSize);
      return;
    }
    // 获得 defaultFetchSize 。非空，则进行设置
    Integer defaultFetchSize = configuration.getDefaultFetchSize();
    if (defaultFetchSize != null) {
      stmt.setFetchSize(defaultFetchSize);
    }
  }

  protected void closeStatement(Statement statement) {
    try {
      if (statement != null) {
        statement.close();
      }
    } catch (SQLException e) {
      //ignore
    }
  }

  /**
   * 创建自增主键
   *
   * @param parameter 参数
   */
  protected void generateKeys(Object parameter) {
    // 主键生成器
    KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
    // 异常处理
    ErrorContext.instance().store();
    // 前置处理，创建自增编号到 parameter 中
    keyGenerator.processBefore(executor, mappedStatement, null, parameter);
    ErrorContext.instance().recall();
  }

}
