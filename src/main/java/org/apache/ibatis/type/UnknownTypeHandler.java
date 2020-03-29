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
package org.apache.ibatis.type;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.Configuration;

/**
 * 未知的 TypeHandler 实现类。
 * @author Clinton Begin
 */
public class UnknownTypeHandler extends BaseTypeHandler<Object> {
  /**
   * ObjectTypeHandler 单例
   */
  private static final ObjectTypeHandler OBJECT_TYPE_HANDLER = new ObjectTypeHandler();
  // TODO Rename to 'configuration' after removing the 'configuration' property(deprecated property) on parent class
  private final Configuration config;
  /**
   * TypeHandler 注册表
   */
  private final Supplier<TypeHandlerRegistry> typeHandlerRegistrySupplier;

  /**
   * The constructor that pass a MyBatis configuration.
   *
   * @param configuration a MyBatis configuration
   * @since 3.5.4
   */
  public UnknownTypeHandler(Configuration configuration) {
    this.config = configuration;
    this.typeHandlerRegistrySupplier = configuration::getTypeHandlerRegistry;
  }

  /**
   * The constructor that pass the type handler registry.
   *
   * @param typeHandlerRegistry a type handler registry
   * @deprecated Since 3.5.4, please use the {@link #UnknownTypeHandler(Configuration)}.
   */
  @Deprecated
  public UnknownTypeHandler(TypeHandlerRegistry typeHandlerRegistry) {
    this.config = new Configuration();
    this.typeHandlerRegistrySupplier = () -> typeHandlerRegistry;
  }

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, Object parameter, JdbcType jdbcType)
    throws SQLException {
    // 1. 获得参数对应的处理器
    TypeHandler handler = resolveTypeHandler(parameter, jdbcType);
    // 使用 handler 设置参数
    handler.setParameter(ps, i, parameter, jdbcType);
  }

  @Override
  public Object getNullableResult(ResultSet rs, String columnName)
    throws SQLException {
    // 1. 获得参数对应的处理器
    TypeHandler<?> handler = resolveTypeHandler(rs, columnName);
    // 使用 handler 获得值
    return handler.getResult(rs, columnName);
  }

  @Override
  public Object getNullableResult(ResultSet rs, int columnIndex)
    throws SQLException {
    // 获得参数对应的处理器
    TypeHandler<?> handler = resolveTypeHandler(rs.getMetaData(), columnIndex);
    // 如果找不到对应的处理器，使用 OBJECT_TYPE_HANDLER
    if (handler == null || handler instanceof UnknownTypeHandler) {
      handler = OBJECT_TYPE_HANDLER;
    }
    // 使用 handler 获得值
    return handler.getResult(rs, columnIndex);
  }

  @Override
  public Object getNullableResult(CallableStatement cs, int columnIndex)
    throws SQLException {
    return cs.getObject(columnIndex);
  }

  private TypeHandler<?> resolveTypeHandler(Object parameter, JdbcType jdbcType) {
    TypeHandler<?> handler;
    // 参数为空，返回 OBJECT_TYPE_HANDLER
    if (parameter == null) {
      handler = OBJECT_TYPE_HANDLER;
    } else {
      // 参数非空，使用参数类型获得对应的 TypeHandler
      handler = typeHandlerRegistrySupplier.get().getTypeHandler(parameter.getClass(), jdbcType);
      // check if handler is null (issue #270)
      // 获取不到，则使用 OBJECT_TYPE_HANDLER
      if (handler == null || handler instanceof UnknownTypeHandler) {
        handler = OBJECT_TYPE_HANDLER;
      }
    }
    return handler;
  }

  private TypeHandler<?> resolveTypeHandler(ResultSet rs, String column) {
    try {
      // 获得 columnIndex
      Map<String, Integer> columnIndexLookup = new HashMap<>();
      ResultSetMetaData rsmd = rs.getMetaData();
      int count = rsmd.getColumnCount();
      boolean useColumnLabel = config.isUseColumnLabel();
      for (int i = 1; i <= count; i++) {
        String name = useColumnLabel ? rsmd.getColumnLabel(i) : rsmd.getColumnName(i);
        columnIndexLookup.put(name, i);
      }
      Integer columnIndex = columnIndexLookup.get(column);
      TypeHandler<?> handler = null;
      // 首先，通过 columnIndex 获得 TypeHandler
      if (columnIndex != null) {
        handler = resolveTypeHandler(rsmd, columnIndex);
      }
      // 获得不到，使用 OBJECT_TYPE_HANDLER
      if (handler == null || handler instanceof UnknownTypeHandler) {
        handler = OBJECT_TYPE_HANDLER;
      }
      return handler;
    } catch (SQLException e) {
      throw new TypeException("Error determining JDBC type for column " + column + ".  Cause: " + e, e);
    }
  }

  private TypeHandler<?> resolveTypeHandler(ResultSetMetaData rsmd, Integer columnIndex) {
    TypeHandler<?> handler = null;
    // 获得 JDBC Type 类型
    JdbcType jdbcType = safeGetJdbcTypeForColumn(rsmd, columnIndex);
    // 获得 Java Type 类型
    Class<?> javaType = safeGetClassForColumn(rsmd, columnIndex);
    //获得对应的 TypeHandler 对象
    if (javaType != null && jdbcType != null) {
      handler = typeHandlerRegistrySupplier.get().getTypeHandler(javaType, jdbcType);
    } else if (javaType != null) {
      handler = typeHandlerRegistrySupplier.get().getTypeHandler(javaType);
    } else if (jdbcType != null) {
      handler = typeHandlerRegistrySupplier.get().getTypeHandler(jdbcType);
    }
    return handler;
  }

  private JdbcType safeGetJdbcTypeForColumn(ResultSetMetaData rsmd, Integer columnIndex) {
    try {
      // 从 ResultSetMetaData 中，获得字段类型，获得 JDBC Type
      return JdbcType.forCode(rsmd.getColumnType(columnIndex));
    } catch (Exception e) {
      return null;
    }
  }

  private Class<?> safeGetClassForColumn(ResultSetMetaData rsmd, Integer columnIndex) {
    try {
      // 从 ResultSetMetaData 中，获得字段类型，获得 Java Type
      return Resources.classForName(rsmd.getColumnClassName(columnIndex));
    } catch (Exception e) {
      return null;
    }
  }
}
