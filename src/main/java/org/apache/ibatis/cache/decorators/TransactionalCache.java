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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;

/**
 * The 2nd level cache transactional buffer.
 * <p>
 * This class holds all cache entries that are to be added to the 2nd level cache during a Session.
 * Entries are sent to the cache when commit is called or discarded if the Session is rolled back.
 * Blocking cache support has been added. Therefore any get() that returns a cache miss
 * will be followed by a put() so any lock associated with the key can be released.
 * <p>
 * 实现 Cache 接口，支持事务的 Cache 实现类，主要用于二级缓存中。
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class TransactionalCache implements Cache {

  private static final Log log = LogFactory.getLog(TransactionalCache.class);

  /**
   * 委托的 Cache 对象。
   * <p>
   * 实际上，就是二级缓存 Cache 对象。
   */
  private final Cache delegate;

  /**
   * todo 真正的意图不太清楚
   * 提交时，清空 {@link #delegate}
   * <p>
   * 初始时，该值为 false
   * 清理后{@link #clear()} 时，该值为 true ，表示持续处于清空状态
   */
  private boolean clearOnCommit;

  /**
   * 待提交的 KV 映射
   */
  private final Map<Object, Object> entriesToAddOnCommit;

  /**
   * 查找不到的 KEY 集合
   */
  private final Set<Object> entriesMissedInCache;

  public TransactionalCache(Cache delegate) {
    this.delegate = delegate;
    this.clearOnCommit = false;
    this.entriesToAddOnCommit = new HashMap<>();
    this.entriesMissedInCache = new HashSet<>();
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  @Override
  public Object getObject(Object key) {
    // issue #116
    // 1. 从 delegate 中获取 key 对应的 value
    Object object = delegate.getObject(key);
    // 2. 如果不存在，则添加到 entriesMissedInCache 中
    if (object == null) {
      entriesMissedInCache.add(key);
    }
    // issue #146
    // 3. 如果 clearOnCommit 为 true ，表示处于持续清空状态，则返回 null
    // 如果 clearOnCommit 为 true ，表示处于持续清空状态，则返回 null 。
    // 因为在事务未结束前，我们执行的清空缓存操作不好同步到 delegate 中，所以只好通过 clearOnCommit 来标记处于清空状态。
    // 那么，如果处于该状态，自然就不能返回 delegate 中查找的结果。
    if (clearOnCommit) {
      return null;
      // 4. 返回 value
    } else {
      return object;
    }
  }

  /**
   * 暂存 KV 到 entriesToAddOnCommit 中
   *
   * @param key    Can be any object but usually it is a {@link CacheKey}
   * @param object
   */
  @Override
  public void putObject(Object key, Object object) {
    entriesToAddOnCommit.put(key, object);
  }

  @Override
  public Object removeObject(Object key) {
    return null;
  }

  /**
   * 清除缓存，只是清除 entriesToAddOnCommit 中占存的数据
   */
  @Override
  public void clear() {
    // 1. 标记 clearOnCommit 为 true
    clearOnCommit = true;
    // 2. 清空 entriesToAddOnCommit
    entriesToAddOnCommit.clear();
  }

  /**
   * 提交缓存
   */
  public void commit() {
    // 1. 如果 clearOnCommit 为 true ，则清空 delegate 缓存
    if (clearOnCommit) {
      delegate.clear();
    }
    // 2. 将 entriesToAddOnCommit、entriesMissedInCache 刷入 delegate 中
    flushPendingEntries();
    // 3. 重置
    reset();
  }

  public void rollback() {
    // 1. 从 delegate 移除出 entriesMissedInCache
    unlockMissedEntries();
    // 2. 重置
    reset();
  }

  /**
   * 重置设置，一个 Executor 可以提交多次事务，而 TransactionalCache 需要被重用，那么就需要重置回初始状态。
   */
  private void reset() {
    // 重置 clearOnCommit 为 false
    clearOnCommit = false;
    // 清空 entriesToAddOnCommit、entriesMissedInCache
    entriesMissedInCache.clear();
  }

  /**
   * 将 entriesToAddOnCommit、entriesMissedInCache 同步到 delegate 中。
   */
  private void flushPendingEntries() {
    for (Map.Entry<Object, Object> entry : entriesToAddOnCommit.entrySet()) {
      delegate.putObject(entry.getKey(), entry.getValue());
    }
    for (Object entry : entriesMissedInCache) {
      if (!entriesToAddOnCommit.containsKey(entry)) {
        delegate.putObject(entry, null);
      }
    }
  }

  private void unlockMissedEntries() {
    for (Object entry : entriesMissedInCache) {
      try {
        delegate.removeObject(entry);
      } catch (Exception e) {
        log.warn("Unexpected exception while notifiying a rollback to the cache adapter. "
          + "Consider upgrading your cache adapter to the latest version. Cause: " + e);
      }
    }
  }

}
