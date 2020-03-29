/**
 * Copyright 2009-2020 the original author or authors.
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
package org.apache.ibatis.cache.decorators;

import java.util.concurrent.TimeUnit;

import org.apache.ibatis.cache.Cache;

/**
 * 定时清空整个容器的 Cache 实现类。
 * 整体实现思路：
 * - {@link #getSize()}|
 * {@link #putObject(Object, Object)}|
 * {@link #getObject(Object)}|
 * {@link #putObject(Object, Object)} 方法在被调用时，首先判断是否要全部清空。
 * - 如果需要则会调用 {@link #clear()} 方法执行清楚工作；
 * - 如果时间未到，则继续下一步。
 *
 * @author Clinton Begin
 */
public class ScheduledCache implements Cache {

  /**
   * 被装饰的 Cache 对象
   */
  private final Cache delegate;
  /**
   * 清空间隔，单位：毫秒
   */
  protected long clearInterval;
  /**
   * 最后清空时间，单位：毫秒
   */
  protected long lastClear;

  public ScheduledCache(Cache delegate) {
    this.delegate = delegate;
    this.clearInterval = TimeUnit.HOURS.toMillis(1);
    this.lastClear = System.currentTimeMillis();
  }

  public void setClearInterval(long clearInterval) {
    this.clearInterval = clearInterval;
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    clearWhenStale();
    return delegate.getSize();
  }

  @Override
  public void putObject(Object key, Object object) {
    clearWhenStale();
    delegate.putObject(key, object);
  }

  @Override
  public Object getObject(Object key) {
    return clearWhenStale() ? null : delegate.getObject(key);
  }

  @Override
  public Object removeObject(Object key) {
    clearWhenStale();
    return delegate.removeObject(key);
  }

  @Override
  public void clear() {
    lastClear = System.currentTimeMillis();
    delegate.clear();
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  private boolean clearWhenStale() {
    // 判断是否要全部清空
    if (System.currentTimeMillis() - lastClear > clearInterval) {
      // 清理数据
      clear();
      return true;
    }
    return false;
  }

}
