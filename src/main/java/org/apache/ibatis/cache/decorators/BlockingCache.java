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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;

/**
 * Simple blocking decorator
 * <p>
 * Simple and inefficient version of EhCache's BlockingCache decorator.
 * It sets a lock over a cache key when the element is not found in cache.
 * This way, other threads will wait until this element is filled instead of hitting the database.
 * <p>
 * 实现 Cache 接口，阻塞的 Cache 实现类。
 *
 * @author Eduardo Macarron
 */
public class BlockingCache implements Cache {

  /**
   * 阻塞等待超时时间
   */
  private long timeout;
  /**
   * 装饰的 Cache 对象
   */
  private final Cache delegate;
  /**
   * 缓存键与 ReentrantLock 对象的映射
   */
  private final ConcurrentHashMap<Object, ReentrantLock> locks;

  public BlockingCache(Cache delegate) {
    this.delegate = delegate;
    this.locks = new ConcurrentHashMap<>();
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
  public void putObject(Object key, Object value) {
    try {
      // 将数据添加到缓存
      delegate.putObject(key, value);
    } finally {
      // 释放锁
      releaseLock(key);
    }
  }

  /**
   * 根据key获取缓存数据
   * <p>
   * 获得缓存值成功时，会释放锁，这样被阻塞等待的其他线程就可以去获取缓存了。
   * 但是，如果获得缓存值失败时，就需要在 #putObject(Object key, Object value) 方法中，
   * 添加缓存时，才会释放锁，这样被阻塞等待的其它线程就不会重复添加缓存了。
   *
   * @param key The key 缓存key
   * @return 缓存中的数据
   */
  @Override
  public Object getObject(Object key) {
    // 对缓存添加锁
    acquireLock(key);
    // 获得缓存值
    Object value = delegate.getObject(key);
    if (value != null) {
      // 释放锁
      releaseLock(key);
    }
    return value;
  }

  /**
   * 很特殊，和方法名字有所“冲突”，不会移除对应的缓存，只会移除锁。
   *
   * @param key The key
   * @return 移除当前key的锁
   */
  @Override
  public Object removeObject(Object key) {
    // despite of its name, this method is called only to release locks
    // 释放锁
    releaseLock(key);
    return null;
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  private ReentrantLock getLockForKey(Object key) {
    // 如果存在则返回，否则添加后再返回对应ReentrantLock
    return locks.computeIfAbsent(key, k -> new ReentrantLock());
  }

  private void acquireLock(Object key) {
    Lock lock = getLockForKey(key);
    if (timeout > 0) {
      try {
        // 设置等待锁的超时时间
        boolean acquired = lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        if (!acquired) {
          throw new CacheException("Couldn't get a lock in " + timeout + " for the key " + key + " at the cache " + delegate.getId());
        }
      } catch (InterruptedException e) {
        throw new CacheException("Got interrupted while trying to acquire lock for key " + key, e);
      }
    } else {
      // 立即加锁
      lock.lock();
    }
  }

  private void releaseLock(Object key) {
    // 获取当前key的锁
    ReentrantLock lock = locks.get(key);
    // 如果是创建锁的线程，才释放对应的锁。
    if (lock.isHeldByCurrentThread()) {
      lock.unlock();
    }
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }
}
