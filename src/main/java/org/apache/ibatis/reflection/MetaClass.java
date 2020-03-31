/**
 * Copyright 2009-2018 the original author or authors.
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
package org.apache.ibatis.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

import org.apache.ibatis.reflection.invoker.GetFieldInvoker;
import org.apache.ibatis.reflection.invoker.Invoker;
import org.apache.ibatis.reflection.invoker.MethodInvoker;
import org.apache.ibatis.reflection.property.PropertyTokenizer;

/**
 * 类的元数据，基于 Reflector 和 PropertyTokenizer ，提供对指定类的各种操作。
 * 一个 MetaClass 对象，对应一个 Class 对象。
 *
 * @author Clinton Begin
 */
public class MetaClass {

  private final ReflectorFactory reflectorFactory;
  private final Reflector reflector;

  private MetaClass(Class<?> type, ReflectorFactory reflectorFactory) {
    this.reflectorFactory = reflectorFactory;
    this.reflector = reflectorFactory.findForClass(type);
  }

  /**
   * 创建指定类的 MetaClass 对象。
   *
   * @param type             指定类
   * @param reflectorFactory 反射工厂
   * @return MetaClass
   */
  public static MetaClass forClass(Class<?> type, ReflectorFactory reflectorFactory) {
    return new MetaClass(type, reflectorFactory);
  }

  /**
   * 创建类的指定属性的类的 MetaClass 对象。
   *
   * @param name 属性名称
   * @return MetaClass
   */
  public MetaClass metaClassForProperty(String name) {
    // 通过getter的getTypes获得属性的类
    Class<?> propType = reflector.getGetterType(name);
    // 创建 MetaClass 对象
    return MetaClass.forClass(propType, reflectorFactory);
  }

  /**
   * 根据名称查找属性
   *
   * @param name 名称
   * @return
   */
  public String findProperty(String name) {
    // 构建属性
    StringBuilder prop = buildProperty(name, new StringBuilder());
    return prop.length() > 0 ? prop.toString() : null;
  }

  /**
   * 移除下划线，在查找属性名
   * <code>
   * "select phone_num,card_num from xxx where id=#{id}"
   * </code>
   * 问题是：此方法只是将下划线去掉，并没有转为驼峰格式。
   * 主要是对大小写不敏感，在这里有答案：{@link Reflector#findPropertyName}中：
   * caseInsensitivePropertyMap.get(name.toUpperCase(Locale.ENGLISH));
   *
   * @param name                名称
   * @param useCamelCaseMapping 是否使用驼峰形式
   * @return 属性名
   */
  public String findProperty(String name, boolean useCamelCaseMapping) {
    if (useCamelCaseMapping) {
      name = name.replace("_", "");
    }
    return findProperty(name);
  }

  public String[] getGetterNames() {
    return reflector.getGetablePropertyNames();
  }

  public String[] getSetterNames() {
    return reflector.getSetablePropertyNames();
  }

  public Class<?> getSetterType(String name) {
    PropertyTokenizer prop = new PropertyTokenizer(name);
    if (prop.hasNext()) {
      MetaClass metaProp = metaClassForProperty(prop.getName());
      return metaProp.getSetterType(prop.getChildren());
    } else {
      return reflector.getSetterType(prop.getName());
    }
  }

  public Class<?> getGetterType(String name) {
    // 创建 PropertyTokenizer 对象，对 name 进行分词
    PropertyTokenizer prop = new PropertyTokenizer(name);
    if (prop.hasNext()) {
      // 创建 MetaClass 对象
      MetaClass metaProp = metaClassForProperty(prop);
      // 递归判断子表达式 children ，获得返回值的类型
      return metaProp.getGetterType(prop.getChildren());
    }
    // issue #506. Resolve the type inside a Collection Object
    // 直接获得返回值的类型
    return getGetterType(prop);
  }

  /**
   * 创建属性对应的 {@link MetaClass}
   * metaClassForProperty => getGetterType => getGenericGetterType 。
   *
   * @param prop 属性解析器
   * @return 属性对应 MetaClass
   */
  private MetaClass metaClassForProperty(PropertyTokenizer prop) {
    // 获取属性的 class
    Class<?> propType = getGetterType(prop);
    // 创建属性对应的 MetaClass
    return MetaClass.forClass(propType, reflectorFactory);
  }

  /**
   * 根据属性分词器
   *
   * @param prop
   * @return
   */
  private Class<?> getGetterType(PropertyTokenizer prop) {
    // 在 Reflector中获取属性名称 class
    Class<?> type = reflector.getGetterType(prop.getName());
    // 如果获取数组的某个位置的元素，则获取其泛型。
    // 例如说：list[0].field ，那么就会解析 list 是什么类型，这样才好通过该类型，继续获得 field
    if (prop.getIndex() != null && Collection.class.isAssignableFrom(type)) {
      // 获得返回的类型
      Type returnType = getGenericGetterType(prop.getName());
      // 如果是泛型，进行解析真正的类型
      if (returnType instanceof ParameterizedType) {
        Type[] actualTypeArguments = ((ParameterizedType) returnType).getActualTypeArguments();
        // 为什么这里判断大小为 1 呢，因为 Collection 是 Collection<T> ，至多一个。
        if (actualTypeArguments != null && actualTypeArguments.length == 1) {
          returnType = actualTypeArguments[0];
          if (returnType instanceof Class) {
            type = (Class<?>) returnType;
          } else if (returnType instanceof ParameterizedType) {
            type = (Class<?>) ((ParameterizedType) returnType).getRawType();
          }
        }
      }
    }
    return type;
  }

  /**
   * 获取
   *
   * @param propertyName
   * @return
   */
  private Type getGenericGetterType(String propertyName) {
    try {
      // 通过属性名称，获取getter方法代理
      Invoker invoker = reflector.getGetInvoker(propertyName);
      // 如果 MethodInvoker 对象，则说明是 getting 方法，解析方法返回类型
      if (invoker instanceof MethodInvoker) {
        Field _method = MethodInvoker.class.getDeclaredField("method");
        _method.setAccessible(true);
        Method method = (Method) _method.get(invoker);
        return TypeParameterResolver.resolveReturnType(method, reflector.getType());
      } else if (invoker instanceof GetFieldInvoker) {
        // 如果 GetFieldInvoker 对象，则说明是 field ，直接访问
        Field _field = GetFieldInvoker.class.getDeclaredField("field");
        _field.setAccessible(true);
        Field field = (Field) _field.get(invoker);
        return TypeParameterResolver.resolveFieldType(field, reflector.getType());
      }
    } catch (NoSuchFieldException | IllegalAccessException ignored) {
    }
    return null;
  }

  /**
   * 指定属性是否存在对应setter方法
   *
   * @param name 属性名称
   * @return true：存在
   */
  public boolean hasSetter(String name) {
    // 使用分词器将name进行分词
    PropertyTokenizer prop = new PropertyTokenizer(name);
    if (prop.hasNext()) {
      if (reflector.hasSetter(prop.getName())) {
        // 创建当前属性的 MetaClass
        MetaClass metaProp = metaClassForProperty(prop.getName());
        return metaProp.hasSetter(prop.getChildren());
      } else {
        return false;
      }
    } else {
      // 验证此属性是否存在setter方法
      return reflector.hasSetter(prop.getName());
    }
  }

  /**
   * 判断指定属性是否有 getting 方法。
   *
   * @param name 属性名称
   * @return 是否存在getter方法
   */
  public boolean hasGetter(String name) {
    // 对属性进行分词
    PropertyTokenizer prop = new PropertyTokenizer(name);
    // 是否有子表达式
    if (prop.hasNext()) {
      // 是否存在getter方法
      if (reflector.hasGetter(prop.getName())) {
        //
        MetaClass metaProp = metaClassForProperty(prop);
        return metaProp.hasGetter(prop.getChildren());
      } else {
        return false;
      }
    } else {
      return reflector.hasGetter(prop.getName());
    }
  }

  public Invoker getGetInvoker(String name) {
    return reflector.getGetInvoker(name);
  }

  public Invoker getSetInvoker(String name) {
    return reflector.getSetInvoker(name);
  }

  /**
   * 创建 PropertyTokenizer 对象，对 name 进行 分词 。当有子表达式，继续递归调用
   *
   * @param name    name
   * @param builder StringBuilder
   * @return StringBuilder
   */
  private StringBuilder buildProperty(String name, StringBuilder builder) {
    // 创建 PropertyTokenizer 对象，对 name 进行分词
    PropertyTokenizer prop = new PropertyTokenizer(name);
    // 有子表达式
    if (prop.hasNext()) {
      // 获得属性名，并添加到 builder 中
      String propertyName = reflector.findPropertyName(prop.getName());
      // 拼接属性到 builder 中
      if (propertyName != null) {
        builder.append(propertyName);
        builder.append(".");
        // 创建 MetaClass 对象
        MetaClass metaProp = metaClassForProperty(propertyName);
        // 递归解析子表达式 children ，并将结果添加到 builder 中
        metaProp.buildProperty(prop.getChildren(), builder);
      }
      // 如果没有子表达式
    } else {
      // 直接获取属性名称，并添加到builder
      String propertyName = reflector.findPropertyName(name);
      if (propertyName != null) {
        builder.append(propertyName);
      }
    }
    return builder;
  }

  public boolean hasDefaultConstructor() {
    return reflector.hasDefaultConstructor();
  }

}
