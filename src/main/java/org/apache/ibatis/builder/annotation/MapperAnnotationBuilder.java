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
package org.apache.ibatis.builder.annotation;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.CacheNamespace;
import org.apache.ibatis.annotations.CacheNamespaceRef;
import org.apache.ibatis.annotations.Case;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Lang;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Options.FlushCachePolicy;
import org.apache.ibatis.annotations.Property;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectKey;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.TypeDiscriminator;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.UpdateProvider;
import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.CacheRefResolver;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.Discriminator;
import org.apache.ibatis.mapping.FetchType;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultFlag;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.UnknownTypeHandler;

/**
 * 一个 Mapper class 对应一个 MapperAnnotationBuilder
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class MapperAnnotationBuilder {

  /**
   * SQL类型注解结合
   */
  private static final Set<Class<? extends Annotation>> SQL_ANNOTATION_TYPES = new HashSet<>();
  /**
   * SQL类型对应的 Provider
   */
  private static final Set<Class<? extends Annotation>> SQL_PROVIDER_ANNOTATION_TYPES = new HashSet<>();

  /**
   * 当前mybatis的核心配置中心
   */
  private final Configuration configuration;
  private final MapperBuilderAssistant assistant;
  /**
   * Mapper 的 class
   */
  private final Class<?> type;

  static {
    SQL_ANNOTATION_TYPES.add(Select.class);
    SQL_ANNOTATION_TYPES.add(Insert.class);
    SQL_ANNOTATION_TYPES.add(Update.class);
    SQL_ANNOTATION_TYPES.add(Delete.class);

    SQL_PROVIDER_ANNOTATION_TYPES.add(SelectProvider.class);
    SQL_PROVIDER_ANNOTATION_TYPES.add(InsertProvider.class);
    SQL_PROVIDER_ANNOTATION_TYPES.add(UpdateProvider.class);
    SQL_PROVIDER_ANNOTATION_TYPES.add(DeleteProvider.class);
  }

  /**
   * 创建 MapperBuilderAssistant 对象
   *
   * @param configuration mybatis 配置
   * @param type          需要被处理的 xxxMapper
   */
  public MapperAnnotationBuilder(Configuration configuration, Class<?> type) {
    // 将包路径转为文件路径
    String resource = type.getName().replace('.', '/') + ".java (best guess)";
    this.assistant = new MapperBuilderAssistant(configuration, resource);
    this.configuration = configuration;
    this.type = type;
  }

  /**
   * 解析Mapper接口配置
   * todo 继续
   */
  public void parse() {
    // 判断当前 Mapper 接口是否应加载过。
    String resource = type.toString();
    if (!configuration.isResourceLoaded(resource)) {
      // 1. 加载对应的 XML Mapper，如果已经加载此接口对应的Mapper.xml，则跳过加载。
      loadXmlResource();
      // 2. 标记该 Mapper 接口已经加载过
      configuration.addLoadedResource(resource);
      // 3. 设置 namespace 属性
      assistant.setCurrentNamespace(type.getName());
      // 4. 解析 @CacheNamespace 注解，并创建缓存
      parseCache();
      // 5. 解析 @CacheNamespaceRef 注解
      parseCacheRef();
      // 6. 遍历每个方法，解析其上的注解
      Method[] methods = type.getMethods();
      for (Method method : methods) {
        try {
          // issue #237
          // 6.1 解析 Method
          if (!method.isBridge()) {
            parseStatement(method);
          }
        } catch (IncompleteElementException e) {
          // 6.2 解析失败，添加到 configuration 中
          configuration.addIncompleteMethod(new MethodResolver(this, method));
        }
      }
    }
    // 7. 解析待定的方法
    parsePendingMethods();
  }

  /**
   * 1）获得对应的集合；2）遍历集合，执行解析；3）执行成功，则移除出集合；4）执行失败，忽略异常。
   * 当然，实际上，此处还是可能有执行解析失败的情况，
   * 但是随着每一个 Mapper 接口对应的 MapperAnnotationBuilder 执行一次这些方法，逐步逐步就会被全部解析完。
   */
  private void parsePendingMethods() {
    // 获得 MethodResolver 集合，并遍历进行处理
    Collection<MethodResolver> incompleteMethods = configuration.getIncompleteMethods();
    synchronized (incompleteMethods) {
      Iterator<MethodResolver> iter = incompleteMethods.iterator();
      while (iter.hasNext()) {
        try {
          // 执行解析
          iter.next().resolve();
          iter.remove();
        } catch (IncompleteElementException e) {
          // This method is still missing a resource
        }
      }
    }
  }

  /**
   * 加载对应的 XML Mapper 。
   */
  private void loadXmlResource() {
    // Spring may not know the real resource name so we check a flag
    // to prevent loading again a resource twice
    // this flag is set at XMLMapperBuilder#bindMapperForNamespace
    // 1. 判断 Mapper XML 是否已经加载过，如果加载过，就不加载了。
    // 此处，是为了避免和 XMLMapperBuilder#parse() 方法冲突，重复解析
    if (!configuration.isResourceLoaded("namespace:" + type.getName())) {
      String xmlResource = type.getName().replace('.', '/') + ".xml";
      // #1347
      // 2. 获得 InputStream 对象
      InputStream inputStream = type.getResourceAsStream("/" + xmlResource);
      if (inputStream == null) {
        // Search XML mapper that is not in the module but in the classpath.
        try {
          inputStream = Resources.getResourceAsStream(type.getClassLoader(), xmlResource);
        } catch (IOException e2) {
          // ignore, resource is not required
        }
      }
      // 创建 XMLMapperBuilder 对象，执行解析
      if (inputStream != null) {
        XMLMapperBuilder xmlParser = new XMLMapperBuilder(
          inputStream, assistant.getConfiguration(), xmlResource, configuration.getSqlFragments(), type.getName());
        xmlParser.parse();
      }
    }
  }

  /**
   * {@link CacheNamespace} 注解主要用于mybatis二级缓存，等同于<cache>属性。
   * <p>
   * 虽然xml配置和注解的功能基本相同，但是使用@CacheNamespace时候要注意：配置文件和接口注释是不能够配合使用的。
   * 只能通过全注解的方式或者全部通过xml配置文件的方式使用。
   *
   * <pre>
   *     @CacheNamespace(implementation = MybatisRedisCache.class)
   *     public interface UserMapper(
   *         @Select("select * from t_user where user_id = #{userId}")
   *         @Options(useCache = true)
   *         List<User> getUser(User u);
   *     }
   * </pre>
   */
  private void parseCache() {
    // 1. 获得类上的 @CacheNamespace 注解
    CacheNamespace cacheDomain = type.getAnnotation(CacheNamespace.class);
    if (cacheDomain != null) {
      // 缓存配置
      Integer size = cacheDomain.size() == 0 ? null : cacheDomain.size();
      Long flushInterval = cacheDomain.flushInterval() == 0 ? null : cacheDomain.flushInterval();
      Properties props = convertToProperties(cacheDomain.properties());
      // 使用新的缓存配置
      assistant.useNewCache(cacheDomain.implementation(), cacheDomain.eviction(),
        flushInterval, size, cacheDomain.readWrite(), cacheDomain.blocking(), props);
    }
  }

  private Properties convertToProperties(Property[] properties) {
    if (properties.length == 0) {
      return null;
    }
    Properties props = new Properties();
    for (Property property : properties) {
      props.setProperty(property.name(),
        PropertyParser.parse(property.value(), configuration.getVariables()));
    }
    return props;
  }

  /**
   * 解析 {@link CacheNamespaceRef} 注解
   *
   * @CacheNamespaceRef(BreedBaseMapper.class)
   */
  private void parseCacheRef() {
    // 获得类上的 @CacheNamespaceRef 注解
    CacheNamespaceRef cacheDomainRef = type.getAnnotation(CacheNamespaceRef.class);
    if (cacheDomainRef != null) {
      // 获取属性
      Class<?> refType = cacheDomainRef.value();
      String refName = cacheDomainRef.name();
      // 校验，如果 refType 和 refName 都为空，则抛出 BuilderException 异常
      if (refType == void.class && refName.isEmpty()) {
        throw new BuilderException("Should be specified either value() or name() attribute in the @CacheNamespaceRef");
      }
      // 校验，如果 refType 和 refName 都不为空，则抛出 BuilderException 异常
      if (refType != void.class && !refName.isEmpty()) {
        throw new BuilderException("Cannot use both value() and name() attribute in the @CacheNamespaceRef");
      }
      // 获得最终的 namespace 属性
      String namespace = (refType != void.class) ? refType.getName() : refName;
      try {
        // 获得指向的 Cache 对象
        assistant.useCacheRef(namespace);
      } catch (IncompleteElementException e) {
        configuration.addIncompleteCacheRef(new CacheRefResolver(assistant, namespace));
      }
    }
  }

  /**
   * 解析其它注解，返回 resultMapId 属性。
   *
   * @param method Method
   * @return resultMapId
   */
  private String parseResultMap(Method method) {
    // 1. 获得返回类型
    Class<?> returnType = getReturnType(method);
    // 2. 获得 @ConstructorArgs、@Results、@TypeDiscriminator 注解
    Arg[] args = method.getAnnotationsByType(Arg.class);
    Result[] results = method.getAnnotationsByType(Result.class);
    TypeDiscriminator typeDiscriminator = method.getAnnotation(TypeDiscriminator.class);
    // 3. 生成 resultMapId
    String resultMapId = generateResultMapName(method);
    // 4. 生成 ResultMap 对象
    applyResultMap(resultMapId, returnType, args, results, typeDiscriminator);
    return resultMapId;
  }

  /**
   * 生成 resultMapId。
   *
   * @param method 方法 method
   * @return ResultMapId
   */
  private String generateResultMapName(Method method) {
    // 第一种情况，已经声明
    // 如果有 @Results 注解，并且有设置 id 属性，则直接返回。格式为：`${type.name}.${Results.id}` 。
    Results results = method.getAnnotation(Results.class);
    if (results != null && !results.id().isEmpty()) {
      return type.getName() + "." + results.id();
    }
    // 第二种情况，自动生成
    // 获得 suffix 前缀，相当于方法参数构成的签名
    StringBuilder suffix = new StringBuilder();
    for (Class<?> c : method.getParameterTypes()) {
      suffix.append("-");
      suffix.append(c.getSimpleName());
    }
    if (suffix.length() < 1) {
      suffix.append("-void");
    }
    // 拼接返回。格式为 `${type.name}.${method.name}${suffix}` 。
    return type.getName() + "." + method.getName() + suffix;
  }

  /**
   * 创建 ResultMap 对象。
   *
   * @param resultMapId   resultMapId
   * @param returnType    返回类型
   * @param args          参数注解
   * @param results       结果注解
   * @param discriminator TypeDiscriminator注解
   */
  private void applyResultMap(String resultMapId, Class<?> returnType, Arg[] args, Result[] results, TypeDiscriminator discriminator) {
    // 1. 创建 ResultMapping 数组
    List<ResultMapping> resultMappings = new ArrayList<>();
    // 2. 将 @Arg[] 注解数组，解析成对应的 ResultMapping 对象们，并添加到 resultMappings 中。
    applyConstructorArgs(args, returnType, resultMappings);
    // 3. 将 @Result[] 注解数组，解析成对应的 ResultMapping 对象们，并添加到 resultMappings 中。
    applyResults(results, returnType, resultMappings);
    // 4. 创建 Discriminator 对象
    Discriminator disc = applyDiscriminator(resultMapId, returnType, discriminator);
    // TODO add AutoMappingBehaviour
    // 5. ResultMap 对象
    assistant.addResultMap(resultMapId, returnType, null, disc, resultMappings, null);
    // 6. 创建 Discriminator 的 ResultMap 对象们
    createDiscriminatorResultMaps(resultMapId, returnType, discriminator);
  }

  /**
   * 创建 Discriminator 的 ResultMap 对象s们。
   *
   * @param resultMapId
   * @param resultType
   * @param discriminator
   */
  private void createDiscriminatorResultMaps(String resultMapId, Class<?> resultType, TypeDiscriminator discriminator) {
    if (discriminator != null) {
      // 遍历 @Case 注解
      for (Case c : discriminator.cases()) {
        // 创建 @Case 注解的 ResultMap 的编号
        String caseResultMapId = resultMapId + "-" + c.value();
        // 创建 ResultMapping 数组
        List<ResultMapping> resultMappings = new ArrayList<>();
        // issue #136
        // 将 @Arg[] 注解数组，解析成对应的 ResultMapping 对象们，并添加到 resultMappings 中。
        applyConstructorArgs(c.constructArgs(), resultType, resultMappings);
        // 将 @Result[] 注解数组，解析成对应的 ResultMapping 对象们，并添加到 resultMappings 中。
        applyResults(c.results(), resultType, resultMappings);
        // TODO add AutoMappingBehaviour
        // 创建 ResultMap 对象
        assistant.addResultMap(caseResultMapId, c.type(), resultMapId, null, resultMappings, null);
      }
    }
  }

  /**
   * 创建 Discriminator 对象。
   *
   * @param resultMapId
   * @param resultType
   * @param discriminator
   * @return
   */
  private Discriminator applyDiscriminator(String resultMapId, Class<?> resultType, TypeDiscriminator discriminator) {
    if (discriminator != null) {
      // 解析各种属性
      String column = discriminator.column();
      Class<?> javaType = discriminator.javaType() == void.class ? String.class : discriminator.javaType();
      JdbcType jdbcType = discriminator.jdbcType() == JdbcType.UNDEFINED ? null : discriminator.jdbcType();
      @SuppressWarnings("unchecked")
      // 获得 TypeHandler 类
        Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>)
        (discriminator.typeHandler() == UnknownTypeHandler.class ? null : discriminator.typeHandler());
      // 遍历 @Case[] 注解数组，解析成 discriminatorMap 集合
      Case[] cases = discriminator.cases();
      Map<String, String> discriminatorMap = new HashMap<>();
      for (Case c : cases) {
        String value = c.value();
        String caseResultMapId = resultMapId + "-" + value;
        discriminatorMap.put(value, caseResultMapId);
      }
      // 创建 Discriminator 对象
      return assistant.buildDiscriminator(resultType, column, javaType, jdbcType, typeHandler, discriminatorMap);
    }
    return null;
  }

  /**
   * 解析方法上的 SQL 操作相关的注解。
   *
   * @param method xxxMapper 中定义的 数据库操作方法
   */
  void parseStatement(Method method) {
    // 1. 获得参数的类型
    Class<?> parameterTypeClass = getParameterType(method);
    // 2. 获得 LanguageDriver 对象
    LanguageDriver languageDriver = getLanguageDriver(method);
    // 3. 创建 SqlSource 对象
    SqlSource sqlSource = getSqlSourceFromAnnotations(method, parameterTypeClass, languageDriver);
    if (sqlSource != null) {
      // 获得各种属性
      Options options = method.getAnnotation(Options.class);
      final String mappedStatementId = type.getName() + "." + method.getName();
      Integer fetchSize = null;
      Integer timeout = null;
      StatementType statementType = StatementType.PREPARED;
      ResultSetType resultSetType = configuration.getDefaultResultSetType();
      // 获取SQL执行类型
      SqlCommandType sqlCommandType = getSqlCommandType(method);
      boolean isSelect = sqlCommandType == SqlCommandType.SELECT;
      boolean flushCache = !isSelect;
      boolean useCache = isSelect;

      KeyGenerator keyGenerator;
      String keyProperty = null;
      String keyColumn = null;
      // 获得 KeyGenerator 对象
      if (SqlCommandType.INSERT.equals(sqlCommandType) || SqlCommandType.UPDATE.equals(sqlCommandType)) {
        // first check for SelectKey annotation - that overrides everything else
        // 如果有 @SelectKey 注解，则进行处理
        SelectKey selectKey = method.getAnnotation(SelectKey.class);
        if (selectKey != null) {
          // 如果有 @SelectKey 注解，则调用 #handleSelectKeyAnnotation(...) 方法，处理 @SelectKey 注解，生成对应的 SelectKey 对象。
          keyGenerator = handleSelectKeyAnnotation(selectKey, mappedStatementId, getParameterType(method), languageDriver);
          keyProperty = selectKey.keyProperty();
        } else if (options == null) {
          // 如果selectKey == null，如果无 @Options 注解，则根据全局配置处理
          keyGenerator = configuration.isUseGeneratedKeys() ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
        } else {
          // 如果 selectKey == null，如果有 @Options 注解，则使用该注解的配置处理
          keyGenerator = options.useGeneratedKeys() ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
          keyProperty = options.keyProperty();
          keyColumn = options.keyColumn();
        }
      } else {
        // 非插入和更新语句，无需 KeyGenerator 对象，所以使用 NoKeyGenerator 单例。
        keyGenerator = NoKeyGenerator.INSTANCE;
      }

      // 初始化各种属性
      if (options != null) {
        if (FlushCachePolicy.TRUE.equals(options.flushCache())) {
          flushCache = true;
        } else if (FlushCachePolicy.FALSE.equals(options.flushCache())) {
          flushCache = false;
        }
        useCache = options.useCache();
        fetchSize = options.fetchSize() > -1 || options.fetchSize() == Integer.MIN_VALUE ? options.fetchSize() : null; //issue #348
        timeout = options.timeout() > -1 ? options.timeout() : null;
        statementType = options.statementType();
        if (options.resultSetType() != ResultSetType.DEFAULT) {
          resultSetType = options.resultSetType();
        }
      }

      // 获得 resultMapId 编号字符串
      String resultMapId = null;
      ResultMap resultMapAnnotation = method.getAnnotation(ResultMap.class);
      // 如果有 @ResultMap 注解，使用该注解为 resultMapId 属性。因为 @ResultMap 注解的作用，就是注解使用的结果集。
      if (resultMapAnnotation != null) {
        resultMapId = String.join(",", resultMapAnnotation.value());
      } else if (isSelect) {
        // 如果无 @ResultMap 注解，解析其它注解，作为 resultMapId 属性
        resultMapId = parseResultMap(method);
      }

      // 构建 MappedStatement 对象
      assistant.addMappedStatement(
        mappedStatementId,
        sqlSource,
        statementType,
        sqlCommandType,
        fetchSize,
        timeout,
        // ParameterMapID
        null,
        parameterTypeClass,
        resultMapId,
        // 获得返回类型
        getReturnType(method),
        resultSetType,
        flushCache,
        useCache,
        // TODO gcode issue #577
        false,
        keyGenerator,
        keyProperty,
        keyColumn,
        // DatabaseID
        null,
        languageDriver,
        // ResultSets
        options != null ? nullOrEmpty(options.resultSets()) : null);
    }
  }

  /**
   * 获得 LanguageDriver 对象。
   *
   * @param method
   * @return
   */
  private LanguageDriver getLanguageDriver(Method method) {
    // 解析 @Lang 注解，获得对应的类型
    Lang lang = method.getAnnotation(Lang.class);
    Class<? extends LanguageDriver> langClass = null;
    if (lang != null) {
      langClass = lang.value();
    }
    // 获得 LanguageDriver 对象
    // 如果 langClass 为空，即无 @Lang 注解，则会使用默认 LanguageDriver 类型
    return configuration.getLanguageDriver(langClass);
  }

  /**
   * 获取参数类型
   * 1. 如果是多参数，则是 ParamMap 类型
   * 2. 如果是单参数，则是该参数的类型
   *
   * @param method 方法
   * @return 参数类型
   */
  private Class<?> getParameterType(Method method) {
    Class<?> parameterType = null;
    Class<?>[] parameterTypes = method.getParameterTypes();
    // 遍历参数类型数组
    for (Class<?> currentParameterType : parameterTypes) {
      // 排除 RowBounds 和 ResultHandler 两种参数 todo 为什么要排除这两种？
      if (!RowBounds.class.isAssignableFrom(currentParameterType) && !ResultHandler.class.isAssignableFrom(currentParameterType)) {
        if (parameterType == null) {
          parameterType = currentParameterType;
        } else {
          // issue #135
          parameterType = ParamMap.class;
        }
      }
    }
    return parameterType;
  }

  /**
   * 获取方法返回值类型
   * <p>
   * todo 这是一段很酷的代码
   *
   * @param method Method
   * @return 返回值的类型
   */
  private Class<?> getReturnType(Method method) {
    // 1. 获得方法的返回类型
    Class<?> returnType = method.getReturnType();
    // 解析成对应的 Type
    Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, type);

    // 2. 如果 Type 是 Class ，则是：普通类
    if (resolvedReturnType instanceof Class) {
      returnType = (Class<?>) resolvedReturnType;
      // 如果是数组类型，则使用 componentType
      if (returnType.isArray()) {
        returnType = returnType.getComponentType();
      }
      // gcode issue #508
      // 如果返回类型是 void ，则尝试使用 @ResultType 注解
      if (void.class.equals(returnType)) {
        ResultType rt = method.getAnnotation(ResultType.class);
        if (rt != null) {
          returnType = rt.value();
        }
      }

      // 3. 如果 Type 是 ParameterizedType ，则是：泛型
    } else if (resolvedReturnType instanceof ParameterizedType) {
      // 获得泛型 rawType
      ParameterizedType parameterizedType = (ParameterizedType) resolvedReturnType;
      Class<?> rawType = (Class<?>) parameterizedType.getRawType();
      // 如果是 Collection 或者 Cursor 类型时
      if (Collection.class.isAssignableFrom(rawType) || Cursor.class.isAssignableFrom(rawType)) {
        // 获得 <> 中实际类型，比如：List<String> 中 String 类型
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        // 如果 actualTypeArguments 的大小为 1 ，进一步处理
        if (actualTypeArguments != null && actualTypeArguments.length == 1) {
          Type returnTypeParameter = actualTypeArguments[0];
          // 如果返回类型是一般类，则将returnType设置为此类型
          if (returnTypeParameter instanceof Class<?>) {
            returnType = (Class<?>) returnTypeParameter;
            // 如果返回类型仍然是泛型类，返回此泛型类型的真实类型。（当然，此类型仍然可能为泛型类型）
          } else if (returnTypeParameter instanceof ParameterizedType) {
            // (gcode issue #443) actual type can be a also a parameterized type
            returnType = (Class<?>) ((ParameterizedType) returnTypeParameter).getRawType();
            // 如果返回类型是一个数组类型，则返还已数组类型
          } else if (returnTypeParameter instanceof GenericArrayType) {
            Class<?> componentType = (Class<?>) ((GenericArrayType) returnTypeParameter).getGenericComponentType();
            // (gcode issue #525) support List<byte[]>
            returnType = Array.newInstance(componentType, 0).getClass();
          }
        }

        // 4. 如果有 @MapKey 注解，并且是 Map 类型
        /*
        MyBatis使用@MapKey注解接收多个查询记录到Map中
        <select id="getUsers" resultType="java.util.Map"  parameterType="java.util.Map">
          select id,name,sex from t_user
        </select>

        @MapKey("id")
        public Map<Integer,Map<String,Object>>getUsers(Map<String,Object>param);

        返回的结果是：
        {1={name:jasion,sex:1},2={name:jack,sex=1}}
         */
      } else if (method.isAnnotationPresent(MapKey.class) && Map.class.isAssignableFrom(rawType)) {
        // (gcode issue 504) Do not look into Maps if there is not MapKey annotation
        // 获得 <> 中实际类型
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        // 如果 actualTypeArguments 的大小为 2 ，进一步处理。
        // 为什么是 2 ，因为 Map<K, V> 呀，有 K、V 两个泛型
        if (actualTypeArguments != null && actualTypeArguments.length == 2) {
          Type returnTypeParameter = actualTypeArguments[1];
          // 获取第二个泛型的类型为：returnType
          if (returnTypeParameter instanceof Class<?>) {
            returnType = (Class<?>) returnTypeParameter;
          } else if (returnTypeParameter instanceof ParameterizedType) {
            // (gcode issue 443) actual type can be a also a parameterized type
            // 如果返回类型仍然是泛型类，返回此泛型类型的真实类型。（当然，此类型仍然可能为泛型类型）
            returnType = (Class<?>) ((ParameterizedType) returnTypeParameter).getRawType();
          }
        }

        // 5.  如果是 Optional 类型时，获取 Optional<Integer> 中Integer
      } else if (Optional.class.equals(rawType)) {
        // 获得 <> 中实际类型
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        // 因为是 Optional<T> 类型，所以 actualTypeArguments 数组大小是一
        Type returnTypeParameter = actualTypeArguments[0];
        // 如果 <T> 泛型为 Class ，则直接使用 Class
        if (returnTypeParameter instanceof Class<?>) {
          returnType = (Class<?>) returnTypeParameter;
        }
      }
    }

    // 6. 返回返回值类型
    return returnType;
  }

  /**
   * 从注解中，获得 SqlSource 对象。
   * 主要区分 {@link MapperAnnotationBuilder#SQL_ANNOTATION_TYPES} 中的注解
   * 与 {@link MapperAnnotationBuilder#SQL_PROVIDER_ANNOTATION_TYPES} 中的注解
   *
   * @param method         方法
   * @param parameterType  参数类型
   * @param languageDriver
   * @return sqlSource
   */
  private SqlSource getSqlSourceFromAnnotations(Method method, Class<?> parameterType, LanguageDriver languageDriver) {
    try {
      // 1. 获得方法上的 SQL_ANNOTATION_TYPES 和 SQL_PROVIDER_ANNOTATION_TYPES 对应的类型
      Class<? extends Annotation> sqlAnnotationType = getSqlAnnotationType(method);
      Class<? extends Annotation> sqlProviderAnnotationType = getSqlProviderAnnotationType(method);
      // 2. 如果 SQL_ANNOTATION_TYPES 对应的类型非空
      if (sqlAnnotationType != null) {
        // 2.1 sqlAnnotationType 与 sqlProviderAnnotationType 不能同时存在
        if (sqlProviderAnnotationType != null) {
          throw new BindingException("You cannot supply both a static SQL and SqlProvider to method named " + method.getName());
        }
        // 2.2 获得 SQL_ANNOTATION_TYPES 对应的注解
        Annotation sqlAnnotation = method.getAnnotation(sqlAnnotationType);
        // 2.3 获得 value 属性
        final String[] strings = (String[]) sqlAnnotation.getClass().getMethod("value").invoke(sqlAnnotation);
        // 2.4 创建 SqlSource 对象
        return buildSqlSourceFromStrings(strings, parameterType, languageDriver);
        // 3. 如果 SQL_PROVIDER_ANNOTATION_TYPES 对应的类型非空
      } else if (sqlProviderAnnotationType != null) {
        // 3.1 获得 SQL_PROVIDER_ANNOTATION_TYPES 对应的注解
        Annotation sqlProviderAnnotation = method.getAnnotation(sqlProviderAnnotationType);
        // 3.2 创建 ProviderSqlSource 对象
        return new ProviderSqlSource(assistant.getConfiguration(), sqlProviderAnnotation, type, method);
      }
      // 返回空
      return null;
    } catch (Exception e) {
      throw new BuilderException("Could not find value method on SQL annotation.  Cause: " + e, e);
    }
  }

  /**
   * 拼接 SQL
   * <pre>
   *     @Insert( value = "insert into user(name,age) " +
   *                      "value(#{name},#{age})" )
   * </pre>
   *
   * @param strings            注解中的value属性
   * @param parameterTypeClass Method 参数类型
   * @param languageDriver
   * @return SqlSource
   */
  private SqlSource buildSqlSourceFromStrings(String[] strings, Class<?> parameterTypeClass, LanguageDriver languageDriver) {
    final StringBuilder sql = new StringBuilder();
    // 因为SQL容易换行编写，所以会使一个数组
    for (String fragment : strings) {
      sql.append(fragment);
      sql.append(" ");
    }
    // 创建 SqlSource 对象
    return languageDriver.createSqlSource(configuration, sql.toString().trim(), parameterTypeClass);
  }

  /**
   * 根据 Method 中的注解 获取 SQL 类型
   *
   * @param method Method 对象
   * @return SqlCommandType
   */
  private SqlCommandType getSqlCommandType(Method method) {
    // 1. 获得符合 SQL_ANNOTATION_TYPES 类型的注解类型
    Class<? extends Annotation> type = getSqlAnnotationType(method);

    if (type == null) {
      // 获得符合 SQL_PROVIDER_ANNOTATION_TYPES 类型的注解类型
      type = getSqlProviderAnnotationType(method);

      // 找不到，返回 SqlCommandType.UNKNOWN
      if (type == null) {
        return SqlCommandType.UNKNOWN;
      }

      // 转换成对应的枚举
      if (type == SelectProvider.class) {
        type = Select.class;
      } else if (type == InsertProvider.class) {
        type = Insert.class;
      } else if (type == UpdateProvider.class) {
        type = Update.class;
      } else if (type == DeleteProvider.class) {
        type = Delete.class;
      }
    }
    // 转换成对应的枚举
    return SqlCommandType.valueOf(type.getSimpleName().toUpperCase(Locale.ENGLISH));
  }

  private Class<? extends Annotation> getSqlAnnotationType(Method method) {
    return chooseAnnotationType(method, SQL_ANNOTATION_TYPES);
  }

  private Class<? extends Annotation> getSqlProviderAnnotationType(Method method) {
    return chooseAnnotationType(method, SQL_PROVIDER_ANNOTATION_TYPES);
  }

  /**
   * 获得符合指定类型的注解类型
   *
   * @param method 方法
   * @param types  指定类型
   * @return 查到的注解类型
   */
  private Class<? extends Annotation> chooseAnnotationType(Method method, Set<Class<? extends Annotation>> types) {
    for (Class<? extends Annotation> type : types) {
      Annotation annotation = method.getAnnotation(type);
      if (annotation != null) {
        return type;
      }
    }
    return null;
  }

  /**
   * 将 @Result[] 注解数组，解析成对应的 ResultMapping 对象们，并添加到 resultMappings 中。
   *
   * @param results
   * @param resultType
   * @param resultMappings
   */
  private void applyResults(Result[] results, Class<?> resultType, List<ResultMapping> resultMappings) {
    // 遍历 @Result[] 数组
    for (Result result : results) {
      // 创建 ResultFlag 数组
      List<ResultFlag> flags = new ArrayList<>();
      if (result.id()) {
        flags.add(ResultFlag.ID);
      }
      @SuppressWarnings("unchecked")
      // 获得 TypeHandler 类
        Class<? extends TypeHandler<?>> typeHandler =
        (Class<? extends TypeHandler<?>>) ((result.typeHandler() == UnknownTypeHandler.class) ? null : result.typeHandler());
      // 构建 ResultMapping 对象
      ResultMapping resultMapping = assistant.buildResultMapping(
        resultType,
        nullOrEmpty(result.property()),
        nullOrEmpty(result.column()),
        result.javaType() == void.class ? null : result.javaType(),
        result.jdbcType() == JdbcType.UNDEFINED ? null : result.jdbcType(),
        // 判断是否有内嵌的查询。
        hasNestedSelect(result) ? nestedSelectId(result) : null,
        null,
        null,
        null,
        typeHandler,
        flags,
        null,
        null,
        isLazy(result));
      // 添加到 resultMappings 中
      resultMappings.add(resultMapping);
    }
  }

  /**
   * 获得内嵌的查询编号。
   *
   * @param result
   * @return
   */
  private String nestedSelectId(Result result) {
    // 先获得 @One 注解
    String nestedSelect = result.one().select();
    // 获得不到，则再获得 @Many
    if (nestedSelect.length() < 1) {
      nestedSelect = result.many().select();
    }
    // 获得内嵌查询编号，格式为 `{type.name}.${select}`
    if (!nestedSelect.contains(".")) {
      nestedSelect = type.getName() + "." + nestedSelect;
    }
    return nestedSelect;
  }

  /**
   * 判断是否为懒加载
   *
   * @param result
   * @return
   */
  private boolean isLazy(Result result) {
    // 判断是否开启懒加载
    boolean isLazy = configuration.isLazyLoadingEnabled();
    // 如果有 @One 注解，则判断是否懒加载
    if (result.one().select().length() > 0 && FetchType.DEFAULT != result.one().fetchType()) {
      isLazy = result.one().fetchType() == FetchType.LAZY;
      // 如果有 @Many 注解，则判断是否懒加载
    } else if (result.many().select().length() > 0 && FetchType.DEFAULT != result.many().fetchType()) {
      isLazy = result.many().fetchType() == FetchType.LAZY;
    }
    return isLazy;
  }

  /**
   * 判断是否内存查询，判断是否有内嵌的查询。
   *
   * @param result
   * @return
   */
  private boolean hasNestedSelect(Result result) {
    if (result.one().select().length() > 0 && result.many().select().length() > 0) {
      throw new BuilderException("Cannot use both @One and @Many annotations in the same @Result");
    }
    // 判断有 @One 或 @Many 注解
    return result.one().select().length() > 0 || result.many().select().length() > 0;
  }

  /**
   * 将 @Arg[] 注解数组，解析成对应的 ResultMapping 对象们，并添加到 resultMappings 中。
   *
   * @param args
   * @param resultType
   * @param resultMappings
   */
  private void applyConstructorArgs(Arg[] args, Class<?> resultType, List<ResultMapping> resultMappings) {
    // 遍历 @Arg[] 数组
    for (Arg arg : args) {
      // 创建 ResultFlag 数组
      List<ResultFlag> flags = new ArrayList<>();
      flags.add(ResultFlag.CONSTRUCTOR);
      if (arg.id()) {
        flags.add(ResultFlag.ID);
      }
      @SuppressWarnings("unchecked")
      // 获得 TypeHandler 类
        Class<? extends TypeHandler<?>> typeHandler =
        (Class<? extends TypeHandler<?>>) (arg.typeHandler() == UnknownTypeHandler.class ? null : arg.typeHandler());

      // 将当前 @Arg 注解构建成 ResultMapping 对象
      ResultMapping resultMapping = assistant.buildResultMapping(
        resultType,
        nullOrEmpty(arg.name()),
        nullOrEmpty(arg.column()),
        arg.javaType() == void.class ? null : arg.javaType(),
        arg.jdbcType() == JdbcType.UNDEFINED ? null : arg.jdbcType(),
        nullOrEmpty(arg.select()),
        nullOrEmpty(arg.resultMap()),
        null,
        nullOrEmpty(arg.columnPrefix()),
        typeHandler,
        flags,
        null,
        null,
        false);
      // 添加到 resultMappings 中
      resultMappings.add(resultMapping);
    }
  }

  private String nullOrEmpty(String value) {
    return value == null || value.trim().length() == 0 ? null : value;
  }

  /**
   * 处理 @SelectKey 注解，生成对应的 KeyGenerator 对象。
   * <pre>
   *     @SelctKey(
   *        statement="SQL语句",
   *        keyProperty="将SQL语句查询结果存放到keyProperty中去",
   *        before="true表示先查询再插入，false反之",
   *        resultType=int.class
   *     )
   * 例如：
   *     @SelectKey(
   *        statement="select count(1)from user",
   *        keyProperty="id",
   *        before=true,
   *        resultType=int.class
   *     )
   * </pre>
   *
   * @param selectKeyAnnotation selectKey 注解信息
   * @param baseStatementId     此 SQL ID
   * @param parameterTypeClass  参数类型
   * @param languageDriver
   * @return KeyGenerator
   */
  private KeyGenerator handleSelectKeyAnnotation(
    SelectKey selectKeyAnnotation,
    String baseStatementId,
    Class<?> parameterTypeClass,
    LanguageDriver languageDriver) {

    // 获得各种属性和对应的类
    String id = baseStatementId + SelectKeyGenerator.SELECT_KEY_SUFFIX;
    Class<?> resultTypeClass = selectKeyAnnotation.resultType();
    StatementType statementType = selectKeyAnnotation.statementType();
    String keyProperty = selectKeyAnnotation.keyProperty();
    String keyColumn = selectKeyAnnotation.keyColumn();
    // 在SQL执行之前还是之后
    boolean executeBefore = selectKeyAnnotation.before();

    // defaults
    boolean useCache = false;
    KeyGenerator keyGenerator = NoKeyGenerator.INSTANCE;
    Integer fetchSize = null;
    Integer timeout = null;
    boolean flushCache = false;
    String parameterMap = null;
    String resultMap = null;
    ResultSetType resultSetTypeEnum = null;

    // 创建 SqlSource 对象，处理SQL语句
    SqlSource sqlSource = buildSqlSourceFromStrings(selectKeyAnnotation.statement(), parameterTypeClass, languageDriver);
    SqlCommandType sqlCommandType = SqlCommandType.SELECT;

    // 创建当前SQL的 MappedStatement 对象，并添加到 Configuration
    assistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType,
      fetchSize, timeout, parameterMap, parameterTypeClass,
      resultMap, resultTypeClass, resultSetTypeEnum,
      flushCache, useCache, false,
      keyGenerator, keyProperty, keyColumn, null, languageDriver, null);

    // 获得 SelectKeyGenerator 的编号，格式为 `${namespace}.${id}`
    id = assistant.applyCurrentNamespace(id, false);

    // 获得 MappedStatement 对象
    MappedStatement keyStatement = configuration.getMappedStatement(id, false);
    // 创建 SelectKeyGenerator 对象，并添加到 configuration 中
    SelectKeyGenerator answer = new SelectKeyGenerator(keyStatement, executeBefore);
    // 将 keyGenerator 添加到 Configuration 中
    configuration.addKeyGenerator(id, answer);
    return answer;
  }

}
