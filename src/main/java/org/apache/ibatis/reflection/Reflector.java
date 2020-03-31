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
package org.apache.ibatis.reflection;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.ReflectPermission;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.ibatis.reflection.invoker.AmbiguousMethodInvoker;
import org.apache.ibatis.reflection.invoker.GetFieldInvoker;
import org.apache.ibatis.reflection.invoker.Invoker;
import org.apache.ibatis.reflection.invoker.MethodInvoker;
import org.apache.ibatis.reflection.invoker.SetFieldInvoker;
import org.apache.ibatis.reflection.property.PropertyNamer;

/**
 * This class represents a cached set of class definition information that
 * allows for easy mapping between property names and getter/setter methods.
 * <p>
 * 反射器，每个 Reflector 对应一个类。Reflector 会缓存反射操作需要的类的信息，
 * 例如：构造方法、属性名、setting / getting 方法等等。
 *
 * @author Clinton Begin
 */
public class Reflector {

  /**
   * 对应的类
   */
  private final Class<?> type;
  /**
   * 可读属性数组
   */
  private final String[] readablePropertyNames;
  /**
   * 可写属性集合
   */
  private final String[] writablePropertyNames;
  /**
   * 属性对应的 setting 方法的映射。
   * <p>
   * key 为属性名称
   * value 为 Invoker 对象
   */
  private final Map<String, Invoker> setMethods = new HashMap<>();
  /**
   * 属性对应的 getting 方法的映射。
   * <p>
   * key 为属性名称
   * value 为 Invoker 对象
   */
  private final Map<String, Invoker> getMethods = new HashMap<>();
  /**
   * 属性对应的 setting 方法的方法参数类型的映射。{@link #setMethods}
   * <p>
   * key 为属性名称
   * value 为方法参数类型
   */
  private final Map<String, Class<?>> setTypes = new HashMap<>();
  /**
   * 属性对应的 getting 方法的返回值类型的映射。{@link #getMethods}
   * <p>
   * key 为属性名称
   * value 为返回值的类型
   */
  private final Map<String, Class<?>> getTypes = new HashMap<>();
  /**
   * 默认构造方法
   */
  private Constructor<?> defaultConstructor;
  /**
   * 不区分大小写的属性集合
   */
  private Map<String, String> caseInsensitivePropertyMap = new HashMap<>();

  public Reflector(Class<?> clazz) {
    // 设置对应的类
    type = clazz;
    // 1. 初始化 defaultConstructor
    addDefaultConstructor(clazz);
    // 2. 初始化 getMethods 和 getTypes ，通过遍历 getting 方法
    addGetMethods(clazz);
    // 3. 初始化 setMethods 和 setTypes ，通过遍历 setting 方法。
    addSetMethods(clazz);
    // 4. 初始化 getMethods + getTypes 和 setMethods + setTypes ，通过遍历 fields 属性。
    addFields(clazz);
    // 5. 初始化 readablePropertyNames、writeablePropertyNames、caseInsensitivePropertyMap 属性
    readablePropertyNames = getMethods.keySet().toArray(new String[0]);
    writablePropertyNames = setMethods.keySet().toArray(new String[0]);
    for (String propName : readablePropertyNames) {
      caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
    }
    for (String propName : writablePropertyNames) {
      caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
    }
  }

  /**
   * 获取指定类的无参构造器
   *
   * @param clazz 指定类class
   */
  private void addDefaultConstructor(Class<?> clazz) {
    // 获取所有public的方法
    Constructor<?>[] constructors = clazz.getDeclaredConstructors();
    // 获取无参构造器
    Arrays.stream(constructors)
      .filter(constructor -> constructor.getParameterTypes().length == 0)
      .findAny()
      .ifPresent(constructor -> this.defaultConstructor = constructor);
  }

  /**
   * 获取指定类的get方法
   *
   * @param clazz 指定类class
   */
  private void addGetMethods(Class<?> clazz) {
    // 属性与其 getting 方法的映射。
    // 存在继承关系，所以同一个getXXX方法可能会被重写多次，就会存在同一个属性对应多个方法的现象。
    Map<String, List<Method>> conflictingGetters = new HashMap<>();
    // 获取所有的方法（去重）
    Method[] methods = getClassMethods(clazz);
    Arrays.stream(methods)
      // 参数大于 0 ，说明不是 getter 方法，忽略
      // 以 get 和 is 方法名开头，说明是 getter 方法
      .filter(m -> m.getParameterTypes().length == 0 && PropertyNamer.isGetter(m.getName()))
      // 获取属性名称，并添加到 conflictingGetters
      .forEach(m -> addMethodConflict(conflictingGetters, PropertyNamer.methodToProperty(m.getName()), m));
    // 解决 getting 冲突方法，并添加至：getMethods 和 getTypes
    resolveGetterConflicts(conflictingGetters);
  }

  /**
   * 解决 getting 冲突方法。最终，一个属性，只保留一个对应的方法。
   * <p>
   * 主要的工作：
   * - 返回值是否为Boolean，如果是则选择is开头方法
   * - 选择子类中的getter方法
   *
   * @param conflictingGetters 包含冲突的getter方法
   */
  private void resolveGetterConflicts(Map<String, List<Method>> conflictingGetters) {
    // 遍历每个属性，查找其最匹配的方法。因为子类可以覆写父类的方法，所以一个属性，可能对应多个 getting 方法
    for (Entry<String, List<Method>> entry : conflictingGetters.entrySet()) {
      // 匹配的方法
      Method winner = null;
      String propName = entry.getKey();
      boolean isAmbiguous = false;
      for (Method candidate : entry.getValue()) {
        // winner 为空，说明第一次遍历，则初始化winner
        if (winner == null) {
          winner = candidate;
          continue;
        }
        // 基于返回类型比较，为什么要基于返回类型比较呢？
        Class<?> winnerType = winner.getReturnType();
        Class<?> candidateType = candidate.getReturnType();
        // 类型相同
        if (candidateType.equals(winnerType)) {
          if (!boolean.class.equals(candidateType)) {
            // 返回类型相同，切不是 Boolean类型的，则设置为：模糊（isAmbiguous）
            isAmbiguous = true;
            break;
          } else if (candidate.getName().startsWith("is")) {
            // 如果是 boolean 类型，则选择 is 开头的方法
            winner = candidate;
          }
        } else if (candidateType.isAssignableFrom(winnerType)) {
          // 选择子类。因为子类可以修改放大返回值。例如，父类的一个方法的返回值为 List ，子类对该方法的返回值可以覆写为 ArrayList 。
          // OK getter type is descendant
        } else if (winnerType.isAssignableFrom(candidateType)) {
          winner = candidate;
        } else {
          isAmbiguous = true;
          break;
        }
      }
      //  添加到 getMethods 和 getTypes 中
      addGetMethod(propName, winner, isAmbiguous);
    }
  }

  private void addGetMethod(String name, Method method, boolean isAmbiguous) {
    MethodInvoker invoker = isAmbiguous
      ? new AmbiguousMethodInvoker(method, MessageFormat.format(
      "Illegal overloaded getter method with ambiguous type for property ''{0}'' in class ''{1}''. This breaks the JavaBeans specification and can cause unpredictable results.",
      name, method.getDeclaringClass().getName()))
      : new MethodInvoker(method);
    // 添加到 getMethods 中
    getMethods.put(name, invoker);
    Type returnType = TypeParameterResolver.resolveReturnType(method, type);
    // 添加到 getTypes 中
    getTypes.put(name, typeToClass(returnType));
  }

  private void addSetMethods(Class<?> clazz) {
    Map<String, List<Method>> conflictingSetters = new HashMap<>();
    // 获取所有的方法
    Method[] methods = getClassMethods(clazz);
    Arrays.stream(methods)
      // 过滤条件：只有一个入参 且 setXXX 开头
      .filter(m -> m.getParameterTypes().length == 1 && PropertyNamer.isSetter(m.getName()))
      .forEach(m -> addMethodConflict(conflictingSetters,
        // 获取属性名称
        PropertyNamer.methodToProperty(m.getName()), m)
      );
    // 解决冲突，并添加到：setMethods 和 setTypes
    resolveSetterConflicts(conflictingSetters);
  }

  private void addMethodConflict(Map<String, List<Method>> conflictingMethods, String name, Method method) {
    if (isValidPropertyName(name)) {
      // 属性如果存在则添加到list列表，不存在则创建列表并添加到列表
      List<Method> list = conflictingMethods.computeIfAbsent(name, k -> new ArrayList<>());
      list.add(method);
    }
  }

  private void resolveSetterConflicts(Map<String, List<Method>> conflictingSetters) {
    for (String propName : conflictingSetters.keySet()) {
      // 获取当前属性的 setter 方法
      List<Method> setters = conflictingSetters.get(propName);
      // 获取当前属性的 getter方法的返回值（返回值类型就是setter方法入参的类型）
      Class<?> getterType = getTypes.get(propName);
      // 验证 getter 方法是不是模糊处理的？
      boolean isGetterAmbiguous = getMethods.get(propName) instanceof AmbiguousMethodInvoker;
      boolean isSetterAmbiguous = false;
      Method match = null;
      for (Method setter : setters) {
        // 如果 getter 方法不是模糊处理，并且，getter返回值类型和setter入参类型相同，则选择此方法作为此属性的setter方法。
        if (!isGetterAmbiguous && setter.getParameterTypes()[0].equals(getterType)) {
          // should be the best match
          match = setter;
          break;
        }
        if (!isSetterAmbiguous) {
          match = pickBetterSetter(match, setter, propName);
          isSetterAmbiguous = match == null;
        }
      }
      // 添加到 setMethods 和 setTypes 中
      if (match != null) {
        addSetMethod(propName, match);
      }
    }
  }

  /**
   * 选择合适的setter方法
   *
   * @param match    match方法，目前最适合的setter方法
   * @param setter   setter方法2
   * @param property 属性名
   * @return 最适合的setter方法
   */
  private Method pickBetterSetter(Method match, Method setter, String property) {
    if (match == null) {
      return setter;
    }
    Class<?> paramType1 = match.getParameterTypes()[0];
    Class<?> paramType2 = setter.getParameterTypes()[0];
    // 验证父子关系，并选择子类setter
    if (paramType1.isAssignableFrom(paramType2)) {
      return setter;
    } else if (paramType2.isAssignableFrom(paramType1)) {
      return match;
    }

    // 此处设置的是 模糊 断定的setter方法
    MethodInvoker invoker = new AmbiguousMethodInvoker(match,
      MessageFormat.format(
        "Ambiguous setters defined for property ''{0}'' in class ''{1}'' with types ''{2}'' and ''{3}''.",
        property, setter.getDeclaringClass().getName(), paramType1.getName(), paramType2.getName()));
    setMethods.put(property, invoker);
    Type[] paramTypes = TypeParameterResolver.resolveParamTypes(match, type);
    setTypes.put(property, typeToClass(paramTypes[0]));
    return null;
  }

  /**
   * 设置属性的 setter 信息到 {@link #setMethods} & {@link #setTypes}
   *
   * @param name   参数名称
   * @param method setter方法
   */
  private void addSetMethod(String name, Method method) {
    MethodInvoker invoker = new MethodInvoker(method);
    setMethods.put(name, invoker);
    Type[] paramTypes = TypeParameterResolver.resolveParamTypes(method, type);
    setTypes.put(name, typeToClass(paramTypes[0]));
  }

  private Class<?> typeToClass(Type src) {
    Class<?> result = null;
    // 普通类型，直接使用类
    if (src instanceof Class) {
      result = (Class<?>) src;
      // 泛型类型，使用泛型
    } else if (src instanceof ParameterizedType) {
      result = (Class<?>) ((ParameterizedType) src).getRawType();
      // 泛型数组，获得具体类
    } else if (src instanceof GenericArrayType) {
      Type componentType = ((GenericArrayType) src).getGenericComponentType();
      // 普通类型
      if (componentType instanceof Class) {
        result = Array.newInstance((Class<?>) componentType, 0).getClass();
      } else {
        // 递归该方法，返回类
        Class<?> componentClass = typeToClass(componentType);
        result = Array.newInstance(componentClass, 0).getClass();
      }
    }
    // 都不符合，使用 Object 类
    if (result == null) {
      result = Object.class;
    }
    return result;
  }

  private void addFields(Class<?> clazz) {
    // 获得所有 field
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      // 1. 添加到 setMethods 和 setTypes 中
      if (!setMethods.containsKey(field.getName())) {
        // issue #379 - removed the check for final because JDK 1.5 allows
        // modification of final fields through reflection (JSR-133). (JGB)
        // pr #16 - final static can only be set by the classloader
        int modifiers = field.getModifiers();
        if (!(Modifier.isFinal(modifiers) && Modifier.isStatic(modifiers))) {
          addSetField(field);
        }
      }
      //  2. 添加到 getMethods 和 getTypes 中
      if (!getMethods.containsKey(field.getName())) {
        addGetField(field);
      }
    }
    // 递归处理父类
    if (clazz.getSuperclass() != null) {
      addFields(clazz.getSuperclass());
    }
  }

  private void addSetField(Field field) {
    // 判断是合理的属性
    if (isValidPropertyName(field.getName())) {
      // 添加到 setMethods 中
      setMethods.put(field.getName(), new SetFieldInvoker(field));
      Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
      // 添加到 setTypes 中
      setTypes.put(field.getName(), typeToClass(fieldType));
    }
  }

  private void addGetField(Field field) {
    // 判断是合理的属性
    if (isValidPropertyName(field.getName())) {
      // 添加到 getMethods 中
      getMethods.put(field.getName(), new GetFieldInvoker(field));
      // 添加到 getMethods 中
      Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
      getTypes.put(field.getName(), typeToClass(fieldType));
    }
  }

  private boolean isValidPropertyName(String name) {
    return !(name.startsWith("$") || "serialVersionUID".equals(name) || "class".equals(name));
  }

  /**
   * This method returns an array containing all methods
   * declared in this class and any superclass.
   * We use this method, instead of the simpler <code>Class.getMethods()</code>,
   * because we want to look for private methods as well.
   *
   * @param clazz The class
   * @return An array containing all methods in this class
   */
  private Method[] getClassMethods(Class<?> clazz) {
    // 唯一的方法
    Map<String, Method> uniqueMethods = new HashMap<>();
    Class<?> currentClass = clazz;
    while (currentClass != null && currentClass != Object.class) {
      addUniqueMethods(uniqueMethods, currentClass.getDeclaredMethods());

      // we also need to look for interface methods - because the class may be abstract
      // 因为当前的类存在是抽象类的可能，所以需要记录接口中定义的方法
      Class<?>[] interfaces = currentClass.getInterfaces();
      for (Class<?> anInterface : interfaces) {
        addUniqueMethods(uniqueMethods, anInterface.getMethods());
      }
      // 获得父类
      currentClass = currentClass.getSuperclass();
    }

    // 转换成 Method 数组返回
    Collection<Method> methods = uniqueMethods.values();
    // todo 为什么是一个 new Method[0] 而不是 new Method[methods.size()]？？？
    return methods.toArray(new Method[0]);
  }

  private void addUniqueMethods(Map<String, Method> uniqueMethods, Method[] methods) {
    for (Method currentMethod : methods) {
      // 忽略 bridge 方法，参见 https://www.zhihu.com/question/54895701/answer/141623158 文章
      if (!currentMethod.isBridge()) {
        // 获得方法签名
        String signature = getSignature(currentMethod);
        // check to see if the method is already known
        // if it is known, then an extended class must have
        // overridden a method
        // 当 uniqueMethods 不存在时，进行添加
        if (!uniqueMethods.containsKey(signature)) {
          // 添加到 uniqueMethods 中
          uniqueMethods.put(signature, currentMethod);
        }
      }
    }
  }

  /**
   * 使用字符串来描述方法
   * returnType#methodName:parameterName1,parameterName2
   *
   * @param method 方法
   * @return 组装后的结果
   */
  private String getSignature(Method method) {
    StringBuilder sb = new StringBuilder();
    Class<?> returnType = method.getReturnType();
    if (returnType != null) {
      sb.append(returnType.getName()).append('#');
    }
    sb.append(method.getName());
    Class<?>[] parameters = method.getParameterTypes();
    for (int i = 0; i < parameters.length; i++) {
      sb.append(i == 0 ? ':' : ',').append(parameters[i].getName());
    }
    return sb.toString();
  }

  /**
   * Checks whether can control member accessible.
   *
   * @return If can control member accessible, it return {@literal true}
   * @since 3.5.0
   */
  public static boolean canControlMemberAccessible() {
    try {
      SecurityManager securityManager = System.getSecurityManager();
      if (null != securityManager) {
        securityManager.checkPermission(new ReflectPermission("suppressAccessChecks"));
      }
    } catch (SecurityException e) {
      return false;
    }
    return true;
  }

  /**
   * Gets the name of the class the instance provides information for.
   *
   * @return The class name
   */
  public Class<?> getType() {
    return type;
  }

  public Constructor<?> getDefaultConstructor() {
    if (defaultConstructor != null) {
      return defaultConstructor;
    } else {
      throw new ReflectionException("There is no default constructor for " + type);
    }
  }

  public boolean hasDefaultConstructor() {
    return defaultConstructor != null;
  }

  /**
   * 用于对属性进行setter访问。
   *
   * @param propertyName 属性名称
   * @return 代理对象
   */
  public Invoker getSetInvoker(String propertyName) {
    Invoker method = setMethods.get(propertyName);
    if (method == null) {
      throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
    }
    return method;
  }

  /**
   * 用于对属性进行getter访问。
   *
   * @param propertyName 属性名称
   * @return 代理对象
   */
  public Invoker getGetInvoker(String propertyName) {
    Invoker method = getMethods.get(propertyName);
    if (method == null) {
      throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
    }
    return method;
  }

  /**
   * Gets the type for a property setter.
   *
   * @param propertyName - the name of the property
   * @return The Class of the property setter
   */
  public Class<?> getSetterType(String propertyName) {
    Class<?> clazz = setTypes.get(propertyName);
    if (clazz == null) {
      throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
    }
    return clazz;
  }

  /**
   * Gets the type for a property getter.
   *
   * @param propertyName - the name of the property
   * @return The Class of the property getter
   */
  public Class<?> getGetterType(String propertyName) {
    Class<?> clazz = getTypes.get(propertyName);
    if (clazz == null) {
      throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
    }
    return clazz;
  }

  /**
   * Gets an array of the readable properties for an object.
   *
   * @return The array
   */
  public String[] getGetablePropertyNames() {
    return readablePropertyNames;
  }

  /**
   * Gets an array of the writable properties for an object.
   *
   * @return The array
   */
  public String[] getSetablePropertyNames() {
    return writablePropertyNames;
  }

  /**
   * Check to see if a class has a writable property by name.
   *
   * @param propertyName - the name of the property to check
   * @return True if the object has a writable property by the name
   */
  public boolean hasSetter(String propertyName) {
    return setMethods.keySet().contains(propertyName);
  }

  /**
   * Check to see if a class has a readable property by name.
   *
   * @param propertyName - the name of the property to check
   * @return True if the object has a readable property by the name
   */
  public boolean hasGetter(String propertyName) {
    return getMethods.keySet().contains(propertyName);
  }

  public String findPropertyName(String name) {
    return caseInsensitivePropertyMap.get(name.toUpperCase(Locale.ENGLISH));
  }
}
