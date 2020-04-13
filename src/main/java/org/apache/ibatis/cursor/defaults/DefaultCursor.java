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
package org.apache.ibatis.cursor.defaults;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.resultset.DefaultResultSetHandler;
import org.apache.ibatis.executor.resultset.ResultSetWrapper;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.ResultContext;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * This is the default implementation of a MyBatis Cursor.
 * This implementation is not thread safe.
 * <p>
 * 实现 Cursor 接口，默认 Cursor 实现类。
 *
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public class DefaultCursor<T> implements Cursor<T> {

  /**
   * ResultSetHandler stuff
   */
  /**
   * 默认结果集处理类
   */
  private final DefaultResultSetHandler resultSetHandler;

  /**
   * 结果map映射关系
   */
  private final ResultMap resultMap;

  /**
   * 结果集包装类
   */
  private final ResultSetWrapper rsw;

  /**
   * 物理分页
   */
  private final RowBounds rowBounds;

  /**
   * 对象包装结果处理类
   */
  private final ObjectWrapperResultHandler<T> objectWrapperResultHandler = new ObjectWrapperResultHandler<>();

  /**
   * CursorIterator 对象，游标迭代器。
   */
  private final CursorIterator cursorIterator = new CursorIterator();

  /**
   * 迭代器是否已被打开，不能被多次打开
   * <p>
   * {@link #iterator()}
   */
  private boolean iteratorRetrieved;

  /**
   * 游标状态
   */
  private CursorStatus status = CursorStatus.CREATED;

  /**
   * 已完成映射的行数
   */
  private int indexWithRowBound = -1;

  private enum CursorStatus {

    /**
     * A freshly created cursor, database ResultSet consuming has not started.
     * 处于被创建状态
     */
    CREATED,
    /**
     * A cursor currently in use, database ResultSet consuming has started.
     * 正在使用中
     */
    OPEN,
    /**
     * A closed cursor, not fully consumed.
     * 已关闭，并未完全消费
     */
    CLOSED,
    /**
     * A fully consumed cursor, a consumed cursor is always closed.
     * 已关闭，并且完全消费
     */
    CONSUMED
  }

  /**
   * 创建一个默认的游标
   *
   * @param resultSetHandler 默认结果集处理类
   * @param resultMap        结果映射map
   * @param rsw              结果集包装类
   * @param rowBounds        分页对象
   */
  public DefaultCursor(DefaultResultSetHandler resultSetHandler,
                       ResultMap resultMap,
                       ResultSetWrapper rsw,
                       RowBounds rowBounds) {
    this.resultSetHandler = resultSetHandler;
    this.resultMap = resultMap;
    this.rsw = rsw;
    this.rowBounds = rowBounds;
  }

  @Override
  public boolean isOpen() {
    return status == CursorStatus.OPEN;
  }

  @Override
  public boolean isConsumed() {
    return status == CursorStatus.CONSUMED;
  }

  /**
   * 获取当前的索引
   * 分页偏移量+游标迭代器索引位置
   *
   * @return
   */
  @Override
  public int getCurrentIndex() {
    return rowBounds.getOffset() + cursorIterator.iteratorIndex;
  }

  /**
   * 获取当前游标的迭代器（保证有且仅返回一次 cursorIterator 对象。）
   *
   * @return
   */
  @Override
  public Iterator<T> iterator() {
    // 如果此迭代器已经被获取，则抛出异常
    if (iteratorRetrieved) {
      throw new IllegalStateException("Cannot open more than one iterator on a Cursor");
    }
    // 如果此迭代器已被关闭，则抛出异常
    if (isClosed()) {
      throw new IllegalStateException("A Cursor is already closed.");
    }
    // 设置当前迭代器被已获取
    iteratorRetrieved = true;
    // 返回游标迭代器
    return cursorIterator;
  }

  /**
   * 关闭当前游标
   */
  @Override
  public void close() {
    //如果游标已经关闭直接返回
    //获取结果集，如果结果集不为空，直接关闭，忽略关闭后异常
    //修改游标的状态为关闭
    if (isClosed()) {
      return;
    }

    ResultSet rs = rsw.getResultSet();
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      // ignore
    } finally {
      status = CursorStatus.CLOSED;
    }
  }

  protected T fetchNextUsingRowBound() {
    T result = fetchNextObjectFromDatabase();
    while (objectWrapperResultHandler.fetched && indexWithRowBound < rowBounds.getOffset()) {
      result = fetchNextObjectFromDatabase();
    }
    return result;
  }

  /**
   * 从数据库中获取下一个对象
   *
   * @return
   */
  protected T fetchNextObjectFromDatabase() {
    if (isClosed()) {
      return null;
    }

    // 设置当前状态是游标打开状态
    // 如果结果集包装类不是已经关闭
    // 把结果放入objectWrapperResultHandler对象的result中
    try {
      objectWrapperResultHandler.fetched = false;
      status = CursorStatus.OPEN;
      if (!rsw.getResultSet().isClosed()) {
        resultSetHandler.handleRowValues(rsw, resultMap, objectWrapperResultHandler, RowBounds.DEFAULT, null);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    // 获取对象包装处理的结果
    // 如果结果不为空结果，索引++
    T next = objectWrapperResultHandler.result;
    if (objectWrapperResultHandler.fetched) {
      indexWithRowBound++;
    }
    // No more object or limit reached
    // next为null或者读取条数等于偏移量+限制条数
    if (!objectWrapperResultHandler.fetched || getReadItemsCount() == rowBounds.getOffset() + rowBounds.getLimit()) {
      close();
      status = CursorStatus.CONSUMED;
    }
    //把结果设置为null
    objectWrapperResultHandler.result = null;

    return next;
  }

  /**
   * 判断是否关闭
   * 游标本身处于关闭状态，或者已经取出结果的所有元素
   *
   * @return
   */
  private boolean isClosed() {
    return status == CursorStatus.CLOSED || status == CursorStatus.CONSUMED;
  }

  /**
   * 下一个读取索引位置
   *
   * @return
   */
  private int getReadItemsCount() {
    return indexWithRowBound + 1;
  }

  /**
   * DefaultCursor 的内部静态类，实现 ResultHandler 接口。对象结果集包装类
   *
   * @param <T>
   */
  protected static class ObjectWrapperResultHandler<T> implements ResultHandler<T> {

    /**
     * 结果对象
     */
    protected T result;
    protected boolean fetched;

    @Override
    public void handleResult(ResultContext<? extends T> context) {
      // 1. 设置结果对象
      this.result = context.getResultObject();
      // 2. 暂停
      context.stop();
      fetched = true;
    }
  }

  /**
   * 当前游标所有的迭代任务都由此迭代器完成
   */
  protected class CursorIterator implements Iterator<T> {

    /**
     * Holder for the next object to be returned.
     * 保存下一个将会被返回的对象，提供给 {@link #next()} 返回
     */
    T object;

    /**
     * Index of objects returned using next(), and as such, visible to users.
     * <p>
     * 返回下一个对象的索引
     */
    int iteratorIndex = -1;

    /**
     * 是否有下个
     *
     * @return
     */
    @Override
    public boolean hasNext() {
      // 1. 如果 object 为空，则遍历下一条记录
      if (object == null) {
        object = fetchNextUsingRowBound();
      }

      // 2. 判断 object 是否非空
      return objectWrapperResultHandler.fetched;
    }

    @Override
    public T next() {
      // Fill next with object fetched from hasNext()
      //执行过 haNext() 方法object的值才不会为null
      T next = object;
      //表示没有执行hasNext()方法，所以在获取一次数据

      // 如果 next 为空，则遍历下一条记录
      if (!objectWrapperResultHandler.fetched) {
        next = fetchNextUsingRowBound();
      }

      if (objectWrapperResultHandler.fetched) {
        objectWrapperResultHandler.fetched = false;
        object = null;
        iteratorIndex++;
        return next;
      }
      throw new NoSuchElementException();
    }

    /**
     * 不支持删除对象
     */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove element from Cursor");
    }
  }
}
