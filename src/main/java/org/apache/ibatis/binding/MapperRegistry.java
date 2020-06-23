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
package org.apache.ibatis.binding;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.builder.annotation.MapperAnnotationBuilder;
import org.apache.ibatis.io.ResolverUtil;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;

/**
 * Mapper 注册中心
 * <p>
 * 通过 {@link Configuration} 类注册 {@link MapperProxyFactory} 到 {@link #knownMappers}
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Lasse Voss
 */
public class MapperRegistry {

  /**
   * MyBatis Configuration 对象
   */
  private final Configuration config;

  /**
   * MapperProxyFactory 的映射
   * <p>
   * key：Mapper 接口
   * value: Mapper Proxy
   */
  private final Map<Class<?>, MapperProxyFactory<?>> knownMappers = new HashMap<>();

  public MapperRegistry(Configuration config) {
    this.config = config;
  }

  /**
   * 根据mapper的class对象获取mapper代理对象
   *
   * @param type       mapper的class对象
   * @param sqlSession 执行当前操作的SQLSession
   * @param <T>        mapper类型
   * @return mapper的代理实现类
   */
  @SuppressWarnings("unchecked")
  public <T> T getMapper(Class<T> type, SqlSession sqlSession) {
    // 指定 Mapper Proxy Factory 的泛型
    final MapperProxyFactory<T> mapperProxyFactory = (MapperProxyFactory<T>) knownMappers.get(type);
    if (mapperProxyFactory == null) {
      throw new BindingException("Type " + type + " is not known to the MapperRegistry.");
    }
    try {
      // 创建 Mapper Proxy 对象
      return mapperProxyFactory.newInstance(sqlSession);
    } catch (Exception e) {
      throw new BindingException("Error getting mapper instance. Cause: " + e, e);
    }
  }

  public <T> boolean hasMapper(Class<T> type) {
    return knownMappers.containsKey(type);
  }

  /**
   * 扫描指定包，并将符合的类，添加到 knownMappers 中。
   *
   * @since 3.2.2
   */
  public void addMappers(String packageName) {
    addMappers(packageName, Object.class);
  }

  /**
   * 扫描指定包下的指定类，并添加到 knownMappers 中
   *
   * @since 3.2.2
   */
  public void addMappers(String packageName, Class<?> superType) {
    ResolverUtil<Class<?>> resolverUtil = new ResolverUtil<>();
    // 扫描指定包下的指定类
    resolverUtil.find(new ResolverUtil.IsA(superType), packageName);
    Set<Class<? extends Class<?>>> mapperSet = resolverUtil.getClasses();
    // 遍历，添加到 knownMappers 中
    for (Class<?> mapperClass : mapperSet) {
      addMapper(mapperClass);
    }
  }

  /**
   * 加载 Mapper 接口
   *
   * @param type xxxMapper 接口
   * @param <T>  Mapper 泛型
   */
  public <T> void addMapper(Class<T> type) {
    // 指定mapper必须为接口
    if (type.isInterface()) {
      // 是否已经存在
      if (hasMapper(type)) {
        throw new BindingException("Type " + type + " is already known to the MapperRegistry.");
      }
      boolean loadCompleted = false;
      try {
        // 添加到 knownMappers 中，TODO：重要：为Mapper接口创建代理对象！！！
        knownMappers.put(type, new MapperProxyFactory<>(type));
        // It's important that the type is added before the parser is run,
        // otherwise the binding may automatically be attempted by the mapper parser.
        // If the type is already known, it won't try.

        // todo 解析 Mapper 接口配置的核心入口
        MapperAnnotationBuilder parser = new MapperAnnotationBuilder(config, type);
        parser.parse();
        // 标记加载完成
        loadCompleted = true;
      } finally {
        // 若加载未完成，从 knownMappers 中移除
        if (!loadCompleted) {
          knownMappers.remove(type);
        }
      }
    }
  }

  /**
   * @since 3.2.2
   */
  public Collection<Class<?>> getMappers() {
    return Collections.unmodifiableCollection(knownMappers.keySet());
  }

}
