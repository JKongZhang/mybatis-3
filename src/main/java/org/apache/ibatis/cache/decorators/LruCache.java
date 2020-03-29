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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.ibatis.cache.Cache;

/**
 * Lru (least recently used) cache decorator.
 * <p>
 * 实现 Cache 接口，基于最少使用的淘汰机制的 Cache 实现类。
 *
 * @author Clinton Begin
 */
public class LruCache implements Cache {

  /**
   * 装饰的 Cache 对象
   */
  private final Cache delegate;
  /**
   * 基于 LinkedHashMap 实现淘汰机制
   */
  private Map<Object, Object> keyMap;
  /**
   * 最老的键，即要被淘汰的
   */
  private Object eldestKey;

  public LruCache(Cache delegate) {
    this.delegate = delegate;
    // 初始化 keyMap 对象
    setSize(1024);
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  public void setSize(final int size) {
    // LinkedHashMap的一个构造函数，当参数accessOrder为true时，
    // 即会按照访问顺序排序，最近访问的放在最前，最早访问的放在后面
    keyMap = new LinkedHashMap<Object, Object>(size, .75F, true) {
      private static final long serialVersionUID = 4267176411845948333L;

      /**
       * LinkedHashMap自带的判断是否删除最老的元素方法，默认返回false，即不删除老数据
       * 我们要做的就是重写这个方法，当满足一定条件时删除老数据
       *
       * @param eldest 最老的元素
       * @return 是否需要移除
       */
      @Override
      protected boolean removeEldestEntry(Map.Entry<Object, Object> eldest) {
        boolean tooBig = size() > size;
        if (tooBig) {
          eldestKey = eldest.getKey();
        }
        return tooBig;
      }
    };
  }

  @Override
  public void putObject(Object key, Object value) {
    // 添加到缓存
    delegate.putObject(key, value);
    // 循环 keyMap
    cycleKeyList(key);
  }

  @Override
  public Object getObject(Object key) {
    // 刷新 keyMap 的访问顺序
    keyMap.get(key);
    // 返回真实数据
    return delegate.getObject(key);
  }

  @Override
  public Object removeObject(Object key) {
    return delegate.removeObject(key);
  }

  @Override
  public void clear() {
    delegate.clear();
    keyMap.clear();
  }

  private void cycleKeyList(Object key) {
    // 添加数据，并验证是否需要移除数据
    keyMap.put(key, key);
    if (eldestKey != null) {
      delegate.removeObject(eldestKey);
      eldestKey = null;
    }
  }

}
