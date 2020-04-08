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
package org.apache.ibatis.executor.keygen;

import java.sql.Statement;
import java.util.List;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.RowBounds;

/**
 * 实现 KeyGenerator 接口，基于从数据库查询主键的 KeyGenerator 实现类，适用于 Oracle、PostgreSQL 。
 * <p>
 * 按照 SelectKeyGenerator 的思路，岂不是可以可以接入 SnowFlake 算法，从而实现分布式主键。 todo
 *
 * @author Clinton Begin
 * @author Jeff Butler
 */
public class SelectKeyGenerator implements KeyGenerator {

  public static final String SELECT_KEY_SUFFIX = "!selectKey";
  /**
   * 是否在 before 阶段执行
   * <p>
   * true ：before
   * after ：after
   */
  private final boolean executeBefore;
  /**
   * MappedStatement 对象
   */
  private final MappedStatement keyStatement;

  public SelectKeyGenerator(MappedStatement keyStatement, boolean executeBefore) {
    this.executeBefore = executeBefore;
    this.keyStatement = keyStatement;
  }

  @Override
  public void processBefore(Executor executor, MappedStatement ms, Statement stmt, Object parameter) {
    if (executeBefore) {
      processGeneratedKeys(executor, ms, parameter);
    }
  }

  @Override
  public void processAfter(Executor executor, MappedStatement ms, Statement stmt, Object parameter) {
    if (!executeBefore) {
      processGeneratedKeys(executor, ms, parameter);
    }
  }

  /**
   * 通过数据库生成主键
   * <pre>
   *     <insert id="addLoginLog" parameterType="map" >
   *   		<selectKey  keyProperty="id" resultType="int" order="BEFORE">
   *   			select nvl(max(id),0)+1 from ap_loginlog
   *   		</selectKey>
   *   		insert into ap_loginlog(ID,MEMBER_ID) values(#{id},#{memberId})
   *   </insert>
   * </pre>
   *
   * @param executor  执行器
   * @param ms        此处的 ms与构造器中传入的keyStatement什么区别？
   *                  构造器传入的 keyStatement 是 生成key的 statement，
   *                  此处方法传入的 ms，是当前 SQL 对应的statement。
   * @param parameter 当前执行的SQL参数
   */
  private void processGeneratedKeys(Executor executor, MappedStatement ms, Object parameter) {
    try {
      // 1. 有查询主键的 SQL 语句，即 keyStatement 对象非空
      if (parameter != null && keyStatement != null && keyStatement.getKeyProperties() != null) {
        String[] keyProperties = keyStatement.getKeyProperties();
        final Configuration configuration = ms.getConfiguration();
        final MetaObject metaParam = configuration.newMetaObject(parameter);
        // Do not close keyExecutor.
        // The transaction will be closed by parent executor.
        // 2. 创建执行器，类型为 SimpleExecutor
        Executor keyExecutor = configuration.newExecutor(executor.getTransaction(), ExecutorType.SIMPLE);
        // 3. 执行查询主键的操作
        List<Object> values = keyExecutor.query(keyStatement, parameter, RowBounds.DEFAULT, Executor.NO_RESULT_HANDLER);
        // 4. 判断查询主键的结果情况
        if (values.size() == 0) {
          throw new ExecutorException("SelectKey returned no data.");
        } else if (values.size() > 1) {
          throw new ExecutorException("SelectKey returned more than one value.");
        } else {
          // 创建 MetaObject 对象，访问查询主键的结果
          MetaObject metaResult = configuration.newMetaObject(values.get(0));
          // 单个主键
          if (keyProperties.length == 1) {
            // 设置属性到 metaParam 中，相当于设置到 parameter 中
            if (metaResult.hasGetter(keyProperties[0])) {
              setValue(metaParam, keyProperties[0], metaResult.getValue(keyProperties[0]));
            } else {
              // no getter for the property - maybe just a single value object
              // so try that
              setValue(metaParam, keyProperties[0], values.get(0));
            }
            // 多个主键
          } else {
            // 遍历，进行赋值
            handleMultipleProperties(keyProperties, metaParam, metaResult);
          }
        }
      }
    } catch (ExecutorException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutorException("Error selecting key or setting result to parameter object. Cause: " + e, e);
    }
  }

  /**
   * 遍历处理多个参数赋值
   *
   * @param keyProperties
   * @param metaParam
   * @param metaResult
   */
  private void handleMultipleProperties(String[] keyProperties,
                                        MetaObject metaParam, MetaObject metaResult) {
    String[] keyColumns = keyStatement.getKeyColumns();

    if (keyColumns == null || keyColumns.length == 0) {
      // no key columns specified, just use the property names
      for (String keyProperty : keyProperties) {
        setValue(metaParam, keyProperty, metaResult.getValue(keyProperty));
      }
    } else {
      if (keyColumns.length != keyProperties.length) {
        throw new ExecutorException("If SelectKey has key columns, the number must match the number of key properties.");
      }
      for (int i = 0; i < keyProperties.length; i++) {
        setValue(metaParam, keyProperties[i], metaResult.getValue(keyColumns[i]));
      }
    }
  }

  /**
   * 将查询的key结果设置到
   *
   * @param metaParam 当前执行的SQL的 metaObject 对象
   * @param property  获取的主键的属性的名称
   * @param value     主键值
   */
  private void setValue(MetaObject metaParam, String property, Object value) {
    if (metaParam.hasSetter(property)) {
      metaParam.setValue(property, value);
    } else {
      throw new ExecutorException("No setter found for the keyProperty '" + property + "' in " + metaParam.getOriginalObject().getClass().getName() + ".");
    }
  }
}
