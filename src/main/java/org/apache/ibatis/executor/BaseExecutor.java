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

import static org.apache.ibatis.executor.ExecutionPlaceholder.EXECUTION_PLACEHOLDER;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementUtil;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.jdbc.ConnectionLogger;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * 实现 Executor 接口，提供骨架方法，从而使子类只要实现指定的几个抽象方法即可。
 *
 * @author Clinton Begin
 */
public abstract class BaseExecutor implements Executor {

  private static final Log log = LogFactory.getLog(BaseExecutor.class);
  /**
   * 事务对象
   */
  protected Transaction transaction;
  /**
   * 包装的 Executor 对象
   */
  protected Executor wrapper;

  /**
   * DeferredLoad( 延迟加载 ) 队列
   */
  protected ConcurrentLinkedQueue<DeferredLoad> deferredLoads;
  /**
   * 本地缓存，即一级缓存
   * <p>
   * 使用 MyBatis 开启一次和数据库的会话，MyBatis 会创建出一个 SqlSession 对象表示一次数据库会话，而每个 SqlSession 都会创建一个 Executor 对象。
   * 一个 SqlSession 对象中创建一个本地缓存( localCache )，对于每一次查询，都会尝试根据查询的条件去本地缓存中查找是否在缓存中，
   * 如果在缓存中，就直接从缓存中取出，然后返回给用户；否则，从数据库读取数据，将查询结果存入缓存并返回给用户。
   */
  protected PerpetualCache localCache;
  /**
   * 本地输出类型的参数的缓存
   */
  protected PerpetualCache localOutputParameterCache;
  /**
   * mybatis 配置
   */
  protected Configuration configuration;

  /**
   * 记录嵌套查询的层级
   */
  protected int queryStack;
  /**
   * 是否关闭
   */
  private boolean closed;

  protected BaseExecutor(Configuration configuration, Transaction transaction) {
    this.transaction = transaction;
    this.deferredLoads = new ConcurrentLinkedQueue<>();
    this.localCache = new PerpetualCache("LocalCache");
    this.localOutputParameterCache = new PerpetualCache("LocalOutputParameterCache");
    this.closed = false;
    this.configuration = configuration;
    this.wrapper = this;
  }

  /**
   * 获得事务对象。
   *
   * @return 事务对象
   */
  @Override
  public Transaction getTransaction() {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    return transaction;
  }

  /**
   * 关闭执行器。
   *
   * @param forceRollback 是否强制回滚
   */
  @Override
  public void close(boolean forceRollback) {
    try {
      try {
        // 回滚数据
        rollback(forceRollback);
      } finally {
        // 事务对象不为空，则关闭事务
        if (transaction != null) {
          transaction.close();
        }
      }
    } catch (SQLException e) {
      // Ignore.  There's nothing that can be done at this point.
      log.warn("Unexpected exception on closing transaction.  Cause: " + e);
    } finally {
      // 置空变量
      transaction = null;
      deferredLoads = null;
      localCache = null;
      localOutputParameterCache = null;
      closed = true;
    }
  }

  /**
   * 执行器是否已经关闭
   *
   * @return
   */
  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * 写操作
   *
   * @param ms        每个 <select />、<insert />、<update />、<delete /> 对应一个 MappedStatement 对象
   * @param parameter 参数
   * @return 受影响的数据
   * @throws SQLException e
   */
  @Override
  public int update(MappedStatement ms, Object parameter) throws SQLException {
    ErrorContext.instance().resource(ms.getResource()).activity("executing an update").object(ms.getId());
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    // 清空缓存
    clearLocalCache();
    return doUpdate(ms, parameter);
  }

  /**
   * 刷入批处理语句。
   *
   * @return 批量执行的结果
   * @throws SQLException e
   */
  @Override
  public List<BatchResult> flushStatements() throws SQLException {
    return flushStatements(false);
  }

  public List<BatchResult> flushStatements(boolean isRollBack) throws SQLException {
    // 如果关闭则报错
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    return doFlushStatements(isRollBack);
  }

  /**
   * 查询数据
   *
   * @param ms            每个 <select />、<insert />、<update />、<delete /> 对应一个 MappedStatement 对象
   * @param parameter     参数
   * @param rowBounds     todo
   * @param resultHandler result处理器
   * @param <E>           返回数据泛型
   * @return 查询的数据
   * @throws SQLException e
   */
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler) throws SQLException {
    // 1. 获得 BoundSql 对象
    BoundSql boundSql = ms.getBoundSql(parameter);
    // 2. 创建 CacheKey 对象
    CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);
    // 3. 查询
    return query(ms, parameter, rowBounds, resultHandler, key, boundSql);
  }

  @SuppressWarnings("unchecked")
  /**
   * 查询数据，带 ResultHandler + CacheKey + BoundSql
   */
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler,
                           CacheKey key, BoundSql boundSql) throws SQLException {
    // todo 1. ErrorContext 的作用是什么？？？
    ErrorContext.instance().resource(ms.getResource()).activity("executing a query").object(ms.getId());
    // 2. 如果已经关闭，则抛出异常
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    // 3. 清空本地缓存，如果 queryStack 为零，并且要求清空本地缓存。
    if (queryStack == 0 && ms.isFlushCacheRequired()) {
      clearLocalCache();
    }

    List<E> list;
    try {
      // 4. 查询 queryStack + 1 todo：queryStack的作用是什么？
      queryStack++;
      // 5. 从一级缓存中，获取查询结果
      list = resultHandler == null ? (List<E>) localCache.getObject(key) : null;
      if (list != null) {
        // 5.1 获取到，则进行处理（处理存储过程的情况，所以我们就忽略。）
        handleLocallyCachedOutputParameters(ms, key, parameter, boundSql);
      } else {
        // 5.2 从数据库中，获取查询结果
        list = queryFromDatabase(ms, parameter, rowBounds, resultHandler, key, boundSql);
      }
    } finally {
      // 6. queryStack - 1
      queryStack--;
    }
    // 7. 执行延迟加载
    if (queryStack == 0) {
      for (DeferredLoad deferredLoad : deferredLoads) {
        deferredLoad.load();
      }
      // issue #601
      // 7.1 清空 deferredLoads
      deferredLoads.clear();
      // 如果缓存的范围是 STATEMENT， 则清空缓存
      if (configuration.getLocalCacheScope() == LocalCacheScope.STATEMENT) {
        // issue #482
        // 7.2 如果缓存级别是 LocalCacheScope.STATEMENT ，则进行清理
        clearLocalCache();
      }
    }
    return list;
  }

  /**
   * 执行查询，返回的结果为 Cursor 游标对象。
   *
   * @param ms
   * @param parameter
   * @param rowBounds
   * @param <E>
   * @return
   * @throws SQLException
   */
  @Override
  public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
    // 1.  获得 BoundSql 对象
    BoundSql boundSql = ms.getBoundSql(parameter);
    // 2. 执行查询
    return doQueryCursor(ms, parameter, rowBounds, boundSql);
  }

  @Override
  public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key, Class<?> targetType) {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    DeferredLoad deferredLoad = new DeferredLoad(resultObject, property, key, localCache, configuration, targetType);
    if (deferredLoad.canLoad()) {
      deferredLoad.load();
    } else {
      deferredLoads.add(new DeferredLoad(resultObject, property, key, localCache, configuration, targetType));
    }
  }

  /**
   * 创建 CacheKey 对象。
   *
   * @param ms              sql 解析的结果
   * @param parameterObject
   * @param rowBounds
   * @param boundSql        可执行的SQL
   * @return
   */
  @Override
  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    // 创建CacheKey对象，并在 cacheKey 中设置 id, offset, limit, sql
    CacheKey cacheKey = new CacheKey();
    cacheKey.update(ms.getId());
    cacheKey.update(rowBounds.getOffset());
    cacheKey.update(rowBounds.getLimit());
    cacheKey.update(boundSql.getSql());
    // 设置 ParameterMapping 数组的元素对应的每个 value 到 CacheKey 对象中
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    TypeHandlerRegistry typeHandlerRegistry = ms.getConfiguration().getTypeHandlerRegistry();
    // mimic DefaultParameterHandler logic 逻辑和 DefaultParameterHandler 获取 value 是一致的。
    for (ParameterMapping parameterMapping : parameterMappings) {
      if (parameterMapping.getMode() != ParameterMode.OUT) {
        Object value;
        String propertyName = parameterMapping.getProperty();
        if (boundSql.hasAdditionalParameter(propertyName)) {
          value = boundSql.getAdditionalParameter(propertyName);
        } else if (parameterObject == null) {
          value = null;
        } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
          value = parameterObject;
        } else {
          MetaObject metaObject = configuration.newMetaObject(parameterObject);
          value = metaObject.getValue(propertyName);
        }
        cacheKey.update(value);
      }
    }
    // 设置 Environment.id 到 CacheKey 对象中
    if (configuration.getEnvironment() != null) {
      // issue #176
      cacheKey.update(configuration.getEnvironment().getId());
    }
    return cacheKey;
  }

  /**
   * 判断一级缓存是否存在。
   *
   * @param ms  封装的SQL
   * @param key 缓存key
   * @return true：缓存存在
   */
  @Override
  public boolean isCached(MappedStatement ms, CacheKey key) {
    return localCache.getObject(key) != null;
  }

  /**
   * 提交事务
   *
   * @param required todo 什么意思？
   * @throws SQLException
   */
  @Override
  public void commit(boolean required) throws SQLException {
    // 已经关闭，则抛出 ExecutorException 异常
    if (closed) {
      throw new ExecutorException("Cannot commit, transaction is already closed");
    }
    // 清空本地缓存
    clearLocalCache();
    // 刷入批处理语句
    flushStatements();
    // 是否要求提交事务。如果是，则提交事务。
    if (required) {
      transaction.commit();
    }
  }

  /**
   * 回滚事务
   *
   * @param required todo
   * @throws SQLException
   */
  @Override
  public void rollback(boolean required) throws SQLException {
    if (!closed) {
      try {
        // 清空本地缓存
        clearLocalCache();
        // 刷入批处理语句
        flushStatements(true);
      } finally {
        if (required) {
          // 是否要求回滚事务。如果是，则回滚事务。
          transaction.rollback();
        }
      }
    }
  }

  /**
   * 清除一级缓存
   */
  @Override
  public void clearLocalCache() {
    if (!closed) {
      // 清理 localCache
      localCache.clear();
      // 清理 localOutputParameterCache
      localOutputParameterCache.clear();
    }
  }

  /**
   * 执行写操作。
   *
   * @param ms        每个 <select />、<insert />、<update />、<delete /> 对应一个 MappedStatement 对象
   * @param parameter 参数
   * @return 此次操作受影响的数量
   * @throws SQLException e
   */
  protected abstract int doUpdate(MappedStatement ms, Object parameter)
    throws SQLException;

  /**
   * 刷入批处理语句。
   *
   * @param isRollback 是否回滚数据
   * @return
   * @throws SQLException
   */
  protected abstract List<BatchResult> doFlushStatements(boolean isRollback)
    throws SQLException;

  /**
   * 执行查询操作，由子类实现
   *
   * @param ms
   * @param parameter
   * @param rowBounds
   * @param resultHandler
   * @param boundSql
   * @param <E>
   * @return
   * @throws SQLException
   */
  protected abstract <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                         ResultHandler resultHandler, BoundSql boundSql) throws SQLException;

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
  protected abstract <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql)
    throws SQLException;

  /**
   * 关闭 Statement 对象
   *
   * @param statement s
   */
  protected void closeStatement(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /**
   * Apply a transaction timeout.
   * 设置事务超时时间
   *
   * @param statement a current statement
   * @throws SQLException if a database access error occurs, this method is called on a closed <code>Statement</code>
   * @see StatementUtil#applyTransactionTimeout(Statement, Integer, Integer)
   * @since 3.4.0
   */
  protected void applyTransactionTimeout(Statement statement) throws SQLException {
    StatementUtil.applyTransactionTimeout(statement, statement.getQueryTimeout(), transaction.getTimeout());
  }

  private void handleLocallyCachedOutputParameters(MappedStatement ms, CacheKey key, Object parameter, BoundSql boundSql) {
    if (ms.getStatementType() == StatementType.CALLABLE) {
      final Object cachedParameter = localOutputParameterCache.getObject(key);
      if (cachedParameter != null && parameter != null) {
        final MetaObject metaCachedParameter = configuration.newMetaObject(cachedParameter);
        final MetaObject metaParameter = configuration.newMetaObject(parameter);
        for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
          if (parameterMapping.getMode() != ParameterMode.IN) {
            final String parameterName = parameterMapping.getProperty();
            final Object cachedValue = metaCachedParameter.getValue(parameterName);
            metaParameter.setValue(parameterName, cachedValue);
          }
        }
      }
    }
  }


  /**
   * 从数据库获取数据
   *
   * @param ms            每个 <select />、<insert />、<update />、<delete /> 对应一个 MappedStatement 对象
   * @param parameter
   * @param rowBounds
   * @param resultHandler 结果处理器
   * @param key           缓存key
   * @param boundSql      处理后的SQL
   * @param <E>           查询泛型
   * @return e
   * @throws SQLException e
   */
  private <E> List<E> queryFromDatabase(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                        ResultHandler resultHandler, CacheKey key, BoundSql boundSql)
    throws SQLException {
    List<E> list;
    // 1. 在缓存中，添加占位对象。此处的占位符，和延迟加载有关，可见 `DeferredLoad#canLoad()` 方法
    localCache.putObject(key, EXECUTION_PLACEHOLDER);
    try {
      // 2. 数据库查询数据
      list = doQuery(ms, parameter, rowBounds, resultHandler, boundSql);
    } finally {
      // 3. 从缓存中，移除占位对象(todo 为什么是移除，而不是替换查询的结果呢?)
      localCache.removeObject(key);
    }
    // 4. 添加到缓存中
    localCache.putObject(key, list);
    // 5. 暂时忽略，存储过程相关
    if (ms.getStatementType() == StatementType.CALLABLE) {
      localOutputParameterCache.putObject(key, parameter);
    }
    return list;
  }

  /**
   * 获得 Connection 对象
   *
   * @param statementLog log对象
   * @return conn对象
   * @throws SQLException e
   */
  protected Connection getConnection(Log statementLog) throws SQLException {
    // 通过事务对象获取conn对象
    Connection connection = transaction.getConnection();
    if (statementLog.isDebugEnabled()) {
      // 如果 debug 日志级别，则创建 ConnectionLogger 对象，进行动态代理。
      // 根据日志等级，创建connection的代理对象，主要目的是输出日志信息。
      return ConnectionLogger.newInstance(connection, statementLog, queryStack);
    } else {
      return connection;
    }
  }

  /**
   * 设置包装器。
   *
   * @param wrapper
   */
  @Override
  public void setExecutorWrapper(Executor wrapper) {
    this.wrapper = wrapper;
  }

  private static class DeferredLoad {

    private final MetaObject resultObject;
    private final String property;
    private final Class<?> targetType;
    private final CacheKey key;
    private final PerpetualCache localCache;
    private final ObjectFactory objectFactory;
    private final ResultExtractor resultExtractor;

    // issue #781
    public DeferredLoad(MetaObject resultObject,
                        String property,
                        CacheKey key,
                        PerpetualCache localCache,
                        Configuration configuration,
                        Class<?> targetType) {
      this.resultObject = resultObject;
      this.property = property;
      this.key = key;
      this.localCache = localCache;
      this.objectFactory = configuration.getObjectFactory();
      this.resultExtractor = new ResultExtractor(configuration, objectFactory);
      this.targetType = targetType;
    }

    public boolean canLoad() {
      return localCache.getObject(key) != null && localCache.getObject(key) != EXECUTION_PLACEHOLDER;
    }

    public void load() {
      @SuppressWarnings("unchecked")
      // we suppose we get back a List
        List<Object> list = (List<Object>) localCache.getObject(key);
      Object value = resultExtractor.extractObjectFromList(list, targetType);
      resultObject.setValue(property, value);
    }

  }

}
