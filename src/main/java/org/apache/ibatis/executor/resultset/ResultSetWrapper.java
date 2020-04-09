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
package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.ObjectTypeHandler;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.apache.ibatis.type.UnknownTypeHandler;

/**
 * @author Iwao AVE!
 */
public class ResultSetWrapper {

  /**
   * ResultSet 对象
   */
  private final ResultSet resultSet;
  private final TypeHandlerRegistry typeHandlerRegistry;
  /**
   * 字段的名字的数组
   */
  private final List<String> columnNames = new ArrayList<>();
  /**
   * 字段的 Java Type 的数组
   */
  private final List<String> classNames = new ArrayList<>();
  /**
   * 字段的 JdbcType 的数组
   */
  private final List<JdbcType> jdbcTypes = new ArrayList<>();
  /**
   * TypeHandler 的映射
   * <p>
   * KEY1：字段的名字
   * KEY2：Java 属性类型
   * Value：类型处理器
   */
  private final Map<String, Map<Class<?>, TypeHandler<?>>> typeHandlerMap = new HashMap<>();
  /**
   * 有 mapped 的字段的名字的映射
   * <p>
   * KEY：{@link #getMapKey(ResultMap, String)}
   * VALUE：字段的名字的数组
   *
   * <pre>
   *     <resultMap id="B" type="Object">
   *         <!-- 此处的 column="year" ，就会被添加到 resultMap.mappedColumns 属性上。 -->
   *         <result property="year" column="year"/>
   *     </resultMap>
   *
   *     <select id="testResultMap" parameterType="Integer" resultMap="A">
   *         SELECT * FROM subject
   *     </select>
   * </pre>
   * <p>
   * 在查询的结果中如果已经定义了作为返回Map，那么将会被存储到 mappedColumnNamesMap 中，
   * 否则被查询出来的字段，没有被映射的字段将会被放到 unMappedColumnNamesMap 中，
   */
  private final Map<String, List<String>> mappedColumnNamesMap = new HashMap<>();
  /**
   * 无 mapped 的字段的名字的映射
   * <p>
   * 和 {@link #mappedColumnNamesMap} 相反
   */
  private final Map<String, List<String>> unMappedColumnNamesMap = new HashMap<>();

  public ResultSetWrapper(ResultSet rs, Configuration configuration) throws SQLException {
    super();
    this.typeHandlerRegistry = configuration.getTypeHandlerRegistry();
    this.resultSet = rs;
    // 遍历 ResultSetMetaData 的字段们，解析出 columnNames、jdbcTypes、classNames 属性
    final ResultSetMetaData metaData = rs.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      // 依次设置 columnName，jdbcType，className
      columnNames.add(configuration.isUseColumnLabel() ? metaData.getColumnLabel(i) : metaData.getColumnName(i));
      jdbcTypes.add(JdbcType.forCode(metaData.getColumnType(i)));
      classNames.add(metaData.getColumnClassName(i));
    }
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public List<String> getColumnNames() {
    return this.columnNames;
  }

  public List<String> getClassNames() {
    return Collections.unmodifiableList(classNames);
  }

  public List<JdbcType> getJdbcTypes() {
    return jdbcTypes;
  }

  /**
   * 根据columnName获取jdbcType
   *
   * @param columnName 属性名称
   * @return
   */
  public JdbcType getJdbcType(String columnName) {
    // 因为 columnNames，jdbcTypes，classNames 的实现都是 ArrayList，并且在设置数据时，依次设置完成对应数据，所以下标相同
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnNames.get(i).equalsIgnoreCase(columnName)) {
        return jdbcTypes.get(i);
      }
    }
    return null;
  }

  /**
   * Gets the type handler to use when reading the result set.
   * Tries to get from the TypeHandlerRegistry by searching for the property type.
   * If not found it gets the column JDBC type and tries to get a handler for it.
   * <p>
   * 获得指定字段名的指定 JavaType 类型的 TypeHandler 对象
   *
   * @param propertyType 属性类型
   * @param columnName   字段名称
   * @return 类型处理器
   */
  public TypeHandler<?> getTypeHandler(Class<?> propertyType, String columnName) {
    TypeHandler<?> handler = null;
    // 1. 先从缓存的 typeHandlerMap 中，获得指定字段名的指定 JavaType 类型的 TypeHandler 对象。
    Map<Class<?>, TypeHandler<?>> columnHandlers = typeHandlerMap.get(columnName);
    if (columnHandlers == null) {
      columnHandlers = new HashMap<>();
      // 如果不存在则设置空map
      typeHandlerMap.put(columnName, columnHandlers);
    } else {
      // 根据 属性类型 获取类型处理器
      handler = columnHandlers.get(propertyType);
    }

    // 如果类型处理器
    if (handler == null) {
      // 获得 JdbcType 类型
      JdbcType jdbcType = getJdbcType(columnName);
      // 从 typeHandlerRegistry 获得 TypeHandler 对象
      handler = typeHandlerRegistry.getTypeHandler(propertyType, jdbcType);
      // Replicate logic of UnknownTypeHandler#resolveTypeHandler
      // See issue #59 comment 10
      // 如果获取不到，则再次进行查找
      if (handler == null || handler instanceof UnknownTypeHandler) {
        // 使用 classNames 中的类型，进行继续查找 TypeHandler 对象
        final int index = columnNames.indexOf(columnName);
        final Class<?> javaType = resolveClass(classNames.get(index));
        if (javaType != null && jdbcType != null) {
          handler = typeHandlerRegistry.getTypeHandler(javaType, jdbcType);
        } else if (javaType != null) {
          handler = typeHandlerRegistry.getTypeHandler(javaType);
        } else if (jdbcType != null) {
          handler = typeHandlerRegistry.getTypeHandler(jdbcType);
        }
      }
      // 如果获取不到，则使用 ObjectTypeHandler 对象
      if (handler == null || handler instanceof UnknownTypeHandler) {
        handler = new ObjectTypeHandler();
      }
      // 缓存到 typeHandlerMap 中
      columnHandlers.put(propertyType, handler);
    }
    return handler;
  }

  private Class<?> resolveClass(String className) {
    try {
      // #699 className could be null
      if (className != null) {
        // 如果 className != null，则获取其 class 对象
        return Resources.classForName(className);
      }
    } catch (ClassNotFoundException e) {
      // ignore
    }
    return null;
  }

  /**
   * 初始化有 mapped 和无 mapped的字段的名字数组。
   *
   * @param resultMap
   * @param columnPrefix
   * @throws SQLException
   */
  private void loadMappedAndUnmappedColumnNames(ResultMap resultMap, String columnPrefix) throws SQLException {
    List<String> mappedColumnNames = new ArrayList<>();
    List<String> unmappedColumnNames = new ArrayList<>();
    // 1. 将 columnPrefix 转换成大写，并拼接到 resultMap.mappedColumns 属性上
    final String upperColumnPrefix = columnPrefix == null ? null : columnPrefix.toUpperCase(Locale.ENGLISH);
    final Set<String> mappedColumns = prependPrefixes(resultMap.getMappedColumns(), upperColumnPrefix);
    // 2. 遍历 columnNames 数组，根据是否在 mappedColumns 中，分别添加到 mappedColumnNames 和 unmappedColumnNames 中。
    for (String columnName : columnNames) {
      final String upperColumnName = columnName.toUpperCase(Locale.ENGLISH);
      if (mappedColumns.contains(upperColumnName)) {
        mappedColumnNames.add(upperColumnName);
      } else {
        unmappedColumnNames.add(columnName);
      }
    }
    // 3. 将 mappedColumnNames 和 unmappedColumnNames 结果，添加到 mappedColumnNamesMap 和 unMappedColumnNamesMap 中
    mappedColumnNamesMap.put(getMapKey(resultMap, columnPrefix), mappedColumnNames);
    unMappedColumnNamesMap.put(getMapKey(resultMap, columnPrefix), unmappedColumnNames);
  }

  /**
   * 获得有 mapped 的字段的名字的数组。
   *
   * @param resultMap
   * @param columnPrefix
   * @return
   * @throws SQLException
   */
  public List<String> getMappedColumnNames(ResultMap resultMap, String columnPrefix) throws SQLException {
    // 获得对应的 mapped 数组
    List<String> mappedColumnNames = mappedColumnNamesMap.get(getMapKey(resultMap, columnPrefix));
    if (mappedColumnNames == null) {
      // 初始化
      loadMappedAndUnmappedColumnNames(resultMap, columnPrefix);
      // 重新获得对应的 mapped 数组
      mappedColumnNames = mappedColumnNamesMap.get(getMapKey(resultMap, columnPrefix));
    }
    return mappedColumnNames;
  }

  /**
   * 获得无 mapped 的字段的名字的数组。
   *
   * @param resultMap
   * @param columnPrefix todo columnPrefix 是什么？？？
   * @return
   * @throws SQLException
   */
  public List<String> getUnmappedColumnNames(ResultMap resultMap, String columnPrefix) throws SQLException {
    // 获得对应的 unMapped 数组
    List<String> unMappedColumnNames = unMappedColumnNamesMap.get(getMapKey(resultMap, columnPrefix));
    if (unMappedColumnNames == null) {
      // 初始化
      loadMappedAndUnmappedColumnNames(resultMap, columnPrefix);
      // 重新获得对应的 unMapped 数组
      unMappedColumnNames = unMappedColumnNamesMap.get(getMapKey(resultMap, columnPrefix));
    }
    return unMappedColumnNames;
  }

  private String getMapKey(ResultMap resultMap, String columnPrefix) {
    return resultMap.getId() + ":" + columnPrefix;
  }

  /**
   * 将 columnPrefix 拼接到 resultMap.mappedColumns 属性上。
   *
   * @param columnNames
   * @param prefix
   * @return
   */
  private Set<String> prependPrefixes(Set<String> columnNames, String prefix) {
    // 直接返回 columnNames ，如果符合如下任一情况
    if (columnNames == null || columnNames.isEmpty() || prefix == null || prefix.length() == 0) {
      return columnNames;
    }
    // 拼接前缀 prefix ，然后返回
    final Set<String> prefixed = new HashSet<>();
    for (String columnName : columnNames) {
      prefixed.add(prefix + columnName);
    }
    return prefixed;
  }

}
