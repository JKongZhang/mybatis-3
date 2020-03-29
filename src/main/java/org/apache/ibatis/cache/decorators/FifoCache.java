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
package org.apache.ibatis.cache.decorators;

import java.util.Deque;
import java.util.LinkedList;

import org.apache.ibatis.cache.Cache;

/**
 * FIFO (first in, first out) cache decorator.
 * <p>
 * 实现 Cache 接口，基于先进先出的淘汰机制的 Cache 实现类。
 *
 * @author Clinton Begin
 */
public class FifoCache implements Cache {

  /**
   * 装饰的 Cache 对象
   */
  private final Cache delegate;
  /**
   * 双端队列，记录缓存键的添加
   */
  private final Deque<Object> keyList;
  /**
   * 队列上限
   */
  private int size;

  public FifoCache(Cache delegate) {
    this.delegate = delegate;
    this.keyList = new LinkedList<>();
    // 设置缓存大小
    this.size = 1024;
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public void putObject(Object key, Object value) {
    // 验证缓存中的数据量是否以达到缓存的大小，如果达到这将头部的数据移除掉。并同步删除delegate中的数据。
    cycleKeyList(key);
    delegate.putObject(key, value);
  }

  @Override
  public Object getObject(Object key) {
    return delegate.getObject(key);
  }

  @Override
  public Object removeObject(Object key) {
    // 此处只设计了移除 delegate 中的数据，name就导致 keyList 中被占用一个位置。
    // 笔者觉得应该也要同步删除 keyList 中的数据
    return delegate.removeObject(key);
  }

  @Override
  public void clear() {
    // 清除缓存中的全部数据
    delegate.clear();
    // 清除 keyList 的所有数据
    keyList.clear();
  }

  private void cycleKeyList(Object key) {
    // 在尾部添加当前数据
    keyList.addLast(key);
    // 如果当前数据大于设置的缓存的大小，则移除头部数据
    if (keyList.size() > size) {
      Object oldestKey = keyList.removeFirst();
      // 同步移除delegate中数据
      delegate.removeObject(oldestKey);
    }
  }

}
