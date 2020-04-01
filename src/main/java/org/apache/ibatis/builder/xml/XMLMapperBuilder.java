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
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.CacheRefResolver;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.ResultMapResolver;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.Discriminator;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.ResultFlag;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLMapperBuilder extends BaseBuilder {

  /**
   * 基于 Java XPath 解析器
   */
  private final XPathParser parser;
  /**
   * Mapper 构造器助手
   */
  private final MapperBuilderAssistant builderAssistant;
  /**
   * 可被其他语句引用的可重用语句块的集合
   * <p>
   * 例如：<sql id="userColumns"> ${alias}.id,${alias}.username,${alias}.password </sql>
   * <p>
   * key:id
   * value:sql xNode
   */
  private final Map<String, XNode> sqlFragments;
  /**
   * 资源引用的地址
   */
  private final String resource;

  @Deprecated
  public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
    this(reader, configuration, resource, sqlFragments);
    this.builderAssistant.setCurrentNamespace(namespace);
  }

  @Deprecated
  public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    this(new XPathParser(reader, true, configuration.getVariables(), new XMLMapperEntityResolver()),
      configuration, resource, sqlFragments);
  }

  public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
    this(inputStream, configuration, resource, sqlFragments);
    this.builderAssistant.setCurrentNamespace(namespace);
  }

  public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    this(new XPathParser(inputStream, true, configuration.getVariables(), new XMLMapperEntityResolver()),
      configuration, resource, sqlFragments);
  }

  private XMLMapperBuilder(XPathParser parser, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    super(configuration);
    // 创建 MapperBuilderAssistant 对象
    this.builderAssistant = new MapperBuilderAssistant(configuration, resource);
    this.parser = parser;
    this.sqlFragments = sqlFragments;
    this.resource = resource;
  }

  /**
   * todo mybatis-mapper.xml 核心解析方法
   * mybatis-mapper.xml 解析
   */
  public void parse() {
    // 判断当前 Mapper 是否已经加载过
    if (!configuration.isResourceLoaded(resource)) {
      // 获取 mapper 节点，并解析 <mapper />
      configurationElement(parser.evalNode("/mapper"));
      // 标记该 Mapper 已经加载过
      configuration.addLoadedResource(resource);
      // 绑定 Mapper
      bindMapperForNamespace();
    }
    // 5. 解析待定的 <resultMap /> 节点
    parsePendingResultMaps();
    // 6. 解析待定的 <cache-ref /> 节点
    parsePendingCacheRefs();
    // 7. 解析待定的 SQL 语句的节点
    parsePendingStatements();
  }

  public XNode getSqlFragment(String refid) {
    return sqlFragments.get(refid);
  }

  /**
   * <pre>
   *   <xs:element name="mapper">
   *     <xs:complexType>
   *       <xs:choice maxOccurs="unbounded">
   *         <xs:element ref="cache-ref"/>
   *         <xs:element ref="cache"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="resultMap"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="parameterMap"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="sql"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="insert"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="update"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="delete"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="select"/>
   *       </xs:choice>
   *       <xs:attribute name="namespace"/>
   *     </xs:complexType>
   *   </xs:element>
   * </pre>
   * 解析mapper.xml文件
   *
   * @param context mapper 节点数据
   */
  private void configurationElement(XNode context) {
    try {
      // 1. 获取命名空间
      String namespace = context.getStringAttribute("namespace");
      if (namespace == null || namespace.equals("")) {
        throw new BuilderException("Mapper's namespace cannot be empty");
      }
      // 设置命名空间。 todo 此命名空间在什么位置会起作用？气什么作用？
      builderAssistant.setCurrentNamespace(namespace);
      // 2. 解析 <cache-ref /> 节点
      cacheRefElement(context.evalNode("cache-ref"));
      // 3. 解析 <cache /> 节点
      cacheElement(context.evalNode("cache"));
      // 已废弃！老式风格的参数映射。内联参数是首选,这个元素可能在将来被移除，这里不会记录。
      parameterMapElement(context.evalNodes("/mapper/parameterMap"));
      // 4. 解析 <resultMap /> 节点们
      resultMapElements(context.evalNodes("/mapper/resultMap"));
      // 5. 解析 <sql /> 节点们
      sqlElement(context.evalNodes("/mapper/sql"));
      // 6. 解析 <select /> <insert /> <update /> <delete /> 节点们
      buildStatementFromContext(context.evalNodes("select|insert|update|delete"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing Mapper XML. The XML location is '" + resource + "'. Cause: " + e, e);
    }
  }

  /**
   * 解析 <select />、<insert />、<update />、<delete /> 节点们。
   *
   * @param list
   */
  private void buildStatementFromContext(List<XNode> list) {
    if (configuration.getDatabaseId() != null) {
      buildStatementFromContext(list, configuration.getDatabaseId());
    }
    buildStatementFromContext(list, null);
  }

  private void buildStatementFromContext(List<XNode> list, String requiredDatabaseId) {
    // 遍历 <select /> <insert /> <update /> <delete /> 节点们
    for (XNode context : list) {
      // 创建 XMLStatementBuilder 对象，执行解析
      final XMLStatementBuilder statementParser = new XMLStatementBuilder(configuration, builderAssistant, context, requiredDatabaseId);
      try {
        statementParser.parseStatementNode();
      } catch (IncompleteElementException e) {
        // 解析失败，添加到 configuration 中
        configuration.addIncompleteStatement(statementParser);
      }
    }
  }

  private void parsePendingResultMaps() {
    Collection<ResultMapResolver> incompleteResultMaps = configuration.getIncompleteResultMaps();
    synchronized (incompleteResultMaps) {
      Iterator<ResultMapResolver> iter = incompleteResultMaps.iterator();
      while (iter.hasNext()) {
        try {
          iter.next().resolve();
          iter.remove();
        } catch (IncompleteElementException e) {
          // ResultMap is still missing a resource...
        }
      }
    }
  }

  private void parsePendingCacheRefs() {
    Collection<CacheRefResolver> incompleteCacheRefs = configuration.getIncompleteCacheRefs();
    synchronized (incompleteCacheRefs) {
      Iterator<CacheRefResolver> iter = incompleteCacheRefs.iterator();
      while (iter.hasNext()) {
        try {
          iter.next().resolveCacheRef();
          iter.remove();
        } catch (IncompleteElementException e) {
          // Cache ref is still missing a resource...
        }
      }
    }
  }

  private void parsePendingStatements() {
    Collection<XMLStatementBuilder> incompleteStatements = configuration.getIncompleteStatements();
    synchronized (incompleteStatements) {
      Iterator<XMLStatementBuilder> iter = incompleteStatements.iterator();
      while (iter.hasNext()) {
        try {
          iter.next().parseStatementNode();
          iter.remove();
        } catch (IncompleteElementException e) {
          // Statement is still missing a resource...
        }
      }
    }
  }

  /**
   * <cache-ref namespace="com.someone.application.data.SomeMapper"/>
   * <p>
   * cache-ref – 引用其它命名空间的缓存配置。
   * 解析 cache-ref 节点
   *
   * @param context cache-ref 节点数据
   */
  private void cacheRefElement(XNode context) {
    if (context != null) {
      // 获得指向的 namespace 名字，并添加到 configuration 的 cacheRefMap 中
      configuration.addCacheRef(builderAssistant.getCurrentNamespace(), context.getStringAttribute("namespace"));
      // 创建 CacheRefResolver 对象，并执行解析
      CacheRefResolver cacheRefResolver = new CacheRefResolver(builderAssistant, context.getStringAttribute("namespace"));
      try {
        cacheRefResolver.resolveCacheRef();
      } catch (IncompleteElementException e) {
        // 解析失败，添加到 configuration 的 incompleteCacheRefs 中
        configuration.addIncompleteCacheRef(cacheRefResolver);
      }
    }
  }

  /**
   * 1. 要启用全局的二级缓存，只需要在你的 SQL 映射文件中添加一行：
   * <cache/>
   * <p>
   * 2. 通用设置
   * <cache eviction="FIFO" flushInterval="60000" size="512" readOnly="true"/>
   * eviction:
   * - LRU – 最近最少使用：移除最长时间不被使用的对象。
   * - FIFO – 先进先出：按对象进入缓存的顺序来移除它们。
   * - SOFT – 软引用：基于垃圾回收器状态和软引用规则移除对象。
   * - WEAK – 弱引用：更积极地基于垃圾收集器状态和弱引用规则移除对象。
   * flushInterval:
   * - flushInterval（刷新间隔）
   * size:
   * - size（引用数目）
   * readOnly:
   * - readOnly（只读）属性可以被设置为 true 或 false。
   * <p>
   * 3. 自定义设置
   * 也可以通过实现你自己的缓存，或为其他第三方缓存方案创建适配器，来完全覆盖缓存行为。并且可以设置一些属性
   * <cache type="com.domain.something.MyCustomCache">
   * <property name="cacheFile" value="/tmp/my-custom-cache.tmp"/>
   * </cache>
   *
   * @param context
   */
  private void cacheElement(XNode context) {
    if (context != null) {
      // 1. 获得负责存储的 Cache 实现类
      String type = context.getStringAttribute("type", "PERPETUAL");
      Class<? extends Cache> typeClass = typeAliasRegistry.resolveAlias(type);
      // 2. 获取 数据失效策略
      String eviction = context.getStringAttribute("eviction", "LRU");
      Class<? extends Cache> evictionClass = typeAliasRegistry.resolveAlias(eviction);
      // 3. 获得 flushInterval、size、readWrite、blocking 属性
      Long flushInterval = context.getLongAttribute("flushInterval");
      Integer size = context.getIntAttribute("size");
      boolean readWrite = !context.getBooleanAttribute("readOnly", false);
      boolean blocking = context.getBooleanAttribute("blocking", false);
      // 获取缓存配置Properties
      Properties props = context.getChildrenAsProperties();
      // 创建 Cache 对象
      builderAssistant.useNewCache(typeClass, evictionClass, flushInterval, size, readWrite, blocking, props);
    }
  }

  private void parameterMapElement(List<XNode> list) {
    for (XNode parameterMapNode : list) {
      String id = parameterMapNode.getStringAttribute("id");
      String type = parameterMapNode.getStringAttribute("type");
      Class<?> parameterClass = resolveClass(type);
      List<XNode> parameterNodes = parameterMapNode.evalNodes("parameter");
      List<ParameterMapping> parameterMappings = new ArrayList<>();
      for (XNode parameterNode : parameterNodes) {
        String property = parameterNode.getStringAttribute("property");
        String javaType = parameterNode.getStringAttribute("javaType");
        String jdbcType = parameterNode.getStringAttribute("jdbcType");
        String resultMap = parameterNode.getStringAttribute("resultMap");
        String mode = parameterNode.getStringAttribute("mode");
        String typeHandler = parameterNode.getStringAttribute("typeHandler");
        Integer numericScale = parameterNode.getIntAttribute("numericScale");
        ParameterMode modeEnum = resolveParameterMode(mode);
        Class<?> javaTypeClass = resolveClass(javaType);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        ParameterMapping parameterMapping = builderAssistant.buildParameterMapping(parameterClass, property, javaTypeClass, jdbcTypeEnum, resultMap, modeEnum, typeHandlerClass, numericScale);
        parameterMappings.add(parameterMapping);
      }
      builderAssistant.addParameterMap(id, parameterClass, parameterMappings);
    }
  }

  private void resultMapElements(List<XNode> list) {
    for (XNode resultMapNode : list) {
      try {
        resultMapElement(resultMapNode);
      } catch (IncompleteElementException e) {
        // ignore, it will be retried
      }
    }
  }

  private ResultMap resultMapElement(XNode resultMapNode) {
    return resultMapElement(resultMapNode, Collections.emptyList(), null);
  }

  /**
   * 1. 简单 resultMap 映射
   * <pre>
   *  <resultMap id="userResultMap" type="User">
   *    <id property="id" column="user_id" />
   *    <result property="username" column="user_name"/>
   *    <result property="password" column="hashed_password"/>
   *  </resultMap>
   *
   *  <select id="selectUsers" resultMap="userResultMap">
   *    select user_id, user_name, hashed_password
   *    from some_table
   *    where id = #{id}
   *  </select>
   * </pre>
   * <p>
   * 2. 复杂 resultMap 映射
   * <pre>
   *  <!-- 非常复杂的结果映射 -->
   *  <resultMap id="detailedBlogResultMap" type="Blog">
   *
   *    <constructor>
   *      <idArg column="blog_id" javaType="int"/>
   *    </constructor>
   *
   *    <result property="title" column="blog_title"/>
   *
   *    <association property="author" javaType="Author">
   *      <id property="id" column="author_id"/>
   *      <result property="username" column="author_username"/>
   *      <result property="password" column="author_password"/>
   *      <result property="email" column="author_email"/>
   *      <result property="bio" column="author_bio"/>
   *      <result property="favouriteSection" column="author_favourite_section"/>
   *    </association>
   *
   *    <collection property="posts" ofType="Post">
   *      <id property="id" column="post_id"/>
   *      <result property="subject" column="post_subject"/>
   *      <association property="author" javaType="Author"/>
   *      <collection property="comments" ofType="Comment">
   *        <id property="id" column="comment_id"/>
   *      </collection>
   *      <collection property="tags" ofType="Tag" >
   *        <id property="id" column="tag_id"/>
   *      </collection>
   *      <discriminator javaType="int" column="draft">
   *        <case value="1" resultType="DraftPost"/>
   *      </discriminator>
   *    </collection>
   *
   *  </resultMap>
   * </pre>
   * 解析 resultMap 标签
   *
   * @param resultMapNode            resultMap 节点数据
   * @param additionalResultMappings null
   * @param enclosingType            null
   * @return
   */
  private ResultMap resultMapElement(XNode resultMapNode, List<ResultMapping> additionalResultMappings, Class<?> enclosingType) {
    ErrorContext.instance().activity("processing " + resultMapNode.getValueBasedIdentifier());

    // 1. 获得 type 属性(type -> ofType -> resultType -> javaType)
    String type = resultMapNode.getStringAttribute("type",
      resultMapNode.getStringAttribute("ofType",
        resultMapNode.getStringAttribute("resultType",
          resultMapNode.getStringAttribute("javaType"))));

    // 1.1 获取type的class
    Class<?> typeClass = resolveClass(type);
    if (typeClass == null) {
      typeClass = inheritEnclosingType(resultMapNode, enclosingType);
    }
    Discriminator discriminator = null;
    List<ResultMapping> resultMappings = new ArrayList<>(additionalResultMappings);
    // 2. 遍历 <resultMap /> 的子节点
    List<XNode> resultChildren = resultMapNode.getChildren();
    for (XNode resultChild : resultChildren) {
      if ("constructor".equals(resultChild.getName())) {
        // 2.1 处理 <constructor /> 节点
        processConstructorElement(resultChild, typeClass, resultMappings);
      } else if ("discriminator".equals(resultChild.getName())) {
        // 2.2 处理 <discriminator /> 节点
        discriminator = processDiscriminatorElement(resultChild, typeClass, resultMappings);
      } else {
        // 2.3 处理其它节点
        List<ResultFlag> flags = new ArrayList<>();
        if ("id".equals(resultChild.getName())) {
          flags.add(ResultFlag.ID);
        }
        // 将当前节点构建成 ResultMapping 对象。
        resultMappings.add(buildResultMappingFromContext(resultChild, typeClass, flags));
      }
    }
    // 获得 id 属性
    String id = resultMapNode.getStringAttribute("id",
      resultMapNode.getValueBasedIdentifier());
    // 获得 extends 属性
    String extend = resultMapNode.getStringAttribute("extends");
    // 获得 autoMapping 属性
    Boolean autoMapping = resultMapNode.getBooleanAttribute("autoMapping");
    // 创建 结果集 解析器
    ResultMapResolver resultMapResolver = new ResultMapResolver(builderAssistant, id, typeClass, extend, discriminator, resultMappings, autoMapping);
    try {
      // 执行解析
      return resultMapResolver.resolve();
    } catch (IncompleteElementException e) {
      configuration.addIncompleteResultMap(resultMapResolver);
      throw e;
    }
  }

  protected Class<?> inheritEnclosingType(XNode resultMapNode, Class<?> enclosingType) {
    if ("association".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
      String property = resultMapNode.getStringAttribute("property");
      if (property != null && enclosingType != null) {
        MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
        return metaResultType.getSetterType(property);
      }
    } else if ("case".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
      return enclosingType;
    }
    return null;
  }

  /**
   * 处理 resultMap 中的 constructor 标签
   *
   * @param resultChild    constructor 标签
   * @param resultType     resultMap 的 type 类型
   * @param resultMappings
   */
  private void processConstructorElement(XNode resultChild, Class<?> resultType, List<ResultMapping> resultMappings) {
    // 获取构造器参数
    List<XNode> argChildren = resultChild.getChildren();
    for (XNode argChild : argChildren) {
      // 获得 ResultFlag 集合
      List<ResultFlag> flags = new ArrayList<>();
      flags.add(ResultFlag.CONSTRUCTOR);
      if ("idArg".equals(argChild.getName())) {
        flags.add(ResultFlag.ID);
      }
      // 将当前子节点构建成 ResultMapping 对象，并添加到 resultMappings 中
      resultMappings.add(buildResultMappingFromContext(argChild, resultType, flags));
    }
  }

  /**
   * 示例：
   * <pre>
   * <resultMap id="vehicleResult" type="Vehicle">
   *   <id property="id" column="id" />
   *   <result property="vin" column="vin"/>
   *   <result property="year" column="year"/>
   *   <result property="make" column="make"/>
   *   <result property="model" column="model"/>
   *   <result property="color" column="color"/>
   *   <discriminator javaType="int" column="vehicle_type">
   *     <case value="1" resultMap="carResult"/>
   *     <case value="2" resultMap="truckResult"/>
   *     <case value="3" resultMap="vanResult"/>
   *     <case value="4" resultMap="suvResult"/>
   *   </discriminator>
   * </resultMap>
   * </pre>
   * 在这个示例中，MyBatis 会从结果集中得到每条记录，然后比较它的 vehicle type 值。
   * 如果它匹配任意一个鉴别器的 case，就会使用这个 case 指定的结果映射。
   * 这个过程是互斥的，也就是说，剩余的结果映射将被忽略（除非它是扩展的，我们将在稍后讨论它）。
   * 如果不能匹配任何一个 case，MyBatis 就只会使用鉴别器块外定义的结果映射。
   * <p>
   * 参考 mybatis-mapper.xsd
   * <pre>
   *   <xs:element name="discriminator">
   *     <xs:complexType>
   *       <xs:sequence>
   *         <xs:element maxOccurs="unbounded" ref="case"/>
   *       </xs:sequence>
   *       <xs:attribute name="column"/>
   *       <xs:attribute name="javaType" use="required"/>
   *       <xs:attribute name="jdbcType"/>
   *       <xs:attribute name="typeHandler"/>
   *     </xs:complexType>
   *   </xs:element>
   *   <xs:element name="case">
   *     <xs:complexType>
   *       <xs:sequence>
   *         <xs:element minOccurs="0" ref="constructor"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="id"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="result"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="association"/>
   *         <xs:element minOccurs="0" maxOccurs="unbounded" ref="collection"/>
   *         <xs:element minOccurs="0" ref="discriminator"/>
   *       </xs:sequence>
   *       <xs:attribute name="value" use="required"/>
   *       <xs:attribute name="resultMap"/>
   *       <xs:attribute name="resultType"/>
   *     </xs:complexType>
   *   </xs:element>
   * </pre>
   * <p>
   * 解析 discriminator 标签
   *
   * @param context
   * @param resultType
   * @param resultMappings
   * @return
   */
  private Discriminator processDiscriminatorElement(XNode context, Class<?> resultType, List<ResultMapping> resultMappings) {
    // 获取 discriminator 节点的 属性值：column、javaType、jdbcType、typeHandler
    String column = context.getStringAttribute("column");
    String javaType = context.getStringAttribute("javaType");
    String jdbcType = context.getStringAttribute("jdbcType");
    String typeHandler = context.getStringAttribute("typeHandler");

    // 解析各种属性对应的类
    Class<?> javaTypeClass = resolveClass(javaType);
    Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
    JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
    Map<String, String> discriminatorMap = new HashMap<>();
    // 遍历 <discriminator /> 的子节点，解析成 discriminatorMap 集合
    for (XNode caseChild : context.getChildren()) {
      String value = caseChild.getStringAttribute("value");
      // 不管是此 case 存在 resultMap 还是 resultType，最终都将转为 resultMap存储
      String resultMap = caseChild.getStringAttribute("resultMap", processNestedResultMappings(caseChild, resultMappings, resultType));
      discriminatorMap.put(value, resultMap);
    }
    // 创建 Discriminator 对象
    return builderAssistant.buildDiscriminator(resultType, column, javaTypeClass, jdbcTypeEnum, typeHandlerClass, discriminatorMap);
  }

  /**
   * 这个元素可以用来定义可重用的 SQL 代码片段，以便在其它语句中使用。
   * <pre>
   *    <sql id="userColumns"> ${alias}.id,${alias}.username,${alias}.password </sql>
   *
   *    <!-- 这个 SQL 片段可以在其它语句中使用，例如：-->
   *    <select id="selectUsers" resultType="map">
   *      select
   *        <include refid="userColumns"><property name="alias" value="t1"/></include>,
   *        <include refid="userColumns"><property name="alias" value="t2"/></include>
   *      from some_table t1
   *        cross join some_table t2
   *    </select>
   * </pre>
   * 解析SQL节点
   *
   * @param list sql list
   */
  private void sqlElement(List<XNode> list) {
    if (configuration.getDatabaseId() != null) {
      sqlElement(list, configuration.getDatabaseId());
    }
    sqlElement(list, null);
  }

  private void sqlElement(List<XNode> list, String requiredDatabaseId) {
    for (XNode context : list) {
      // 指定数据源
      String databaseId = context.getStringAttribute("databaseId");
      // 当前 sql 段的ID
      String id = context.getStringAttribute("id");
      // 获得完整的 id 属性，格式为 `${namespace}.${id}` 。
      id = builderAssistant.applyCurrentNamespace(id, false);
      // 判断 databaseId 是否匹配
      if (databaseIdMatchesCurrent(id, databaseId, requiredDatabaseId)) {
        // 添加到 sqlFragments 中
        sqlFragments.put(id, context);
      }
    }
  }

  /**
   * databaseId 是否与 sql 段中的 databaseId一致
   *
   * @param id
   * @param databaseId
   * @param requiredDatabaseId
   * @return
   */
  private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
    if (requiredDatabaseId != null) {
      return requiredDatabaseId.equals(databaseId);
    }
    if (databaseId != null) {
      return false;
    }
    if (!this.sqlFragments.containsKey(id)) {
      return true;
    }
    // skip this fragment if there is a previous one with a not null databaseId
    XNode context = this.sqlFragments.get(id);
    return context.getStringAttribute("databaseId") == null;
  }

  private ResultMapping buildResultMappingFromContext(XNode context, Class<?> resultType, List<ResultFlag> flags) {
    String property;
    if (flags.contains(ResultFlag.CONSTRUCTOR)) {
      property = context.getStringAttribute("name");
    } else {
      property = context.getStringAttribute("property");
    }
    // 解析各种属性
    String column = context.getStringAttribute("column");
    String javaType = context.getStringAttribute("javaType");
    String jdbcType = context.getStringAttribute("jdbcType");
    String nestedSelect = context.getStringAttribute("select");
    String nestedResultMap = context.getStringAttribute("resultMap", () ->
      processNestedResultMappings(context, Collections.emptyList(), resultType));
    String notNullColumn = context.getStringAttribute("notNullColumn");
    String columnPrefix = context.getStringAttribute("columnPrefix");
    String typeHandler = context.getStringAttribute("typeHandler");
    String resultSet = context.getStringAttribute("resultSet");
    String foreignColumn = context.getStringAttribute("foreignColumn");
    boolean lazy = "lazy".equals(context.getStringAttribute("fetchType", configuration.isLazyLoadingEnabled() ? "lazy" : "eager"));
    // 获得各种属性对应的类
    Class<?> javaTypeClass = resolveClass(javaType);
    Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
    JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
    // 构建 ResultMapping 对象
    return builderAssistant.buildResultMapping(resultType, property, column, javaTypeClass, jdbcTypeEnum, nestedSelect, nestedResultMap, notNullColumn, columnPrefix, typeHandlerClass, flags, resultSet, foreignColumn, lazy);
  }

  private String processNestedResultMappings(XNode context, List<ResultMapping> resultMappings, Class<?> enclosingType) {
    if ("association".equals(context.getName())
      || "collection".equals(context.getName())
      || "case".equals(context.getName())) {
      if (context.getStringAttribute("select") == null) {
        validateCollection(context, enclosingType);
        // // 解析，并返回 ResultMap
        ResultMap resultMap = resultMapElement(context, resultMappings, enclosingType);
        return resultMap.getId();
      }
    }
    return null;
  }

  protected void validateCollection(XNode context, Class<?> enclosingType) {
    if ("collection".equals(context.getName()) && context.getStringAttribute("resultMap") == null
      && context.getStringAttribute("javaType") == null) {
      MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
      String property = context.getStringAttribute("property");
      if (!metaResultType.hasSetter(property)) {
        throw new BuilderException(
          "Ambiguous collection type for property '" + property + "'. You must specify 'javaType' or 'resultMap'.");
      }
    }
  }

  private void bindMapperForNamespace() {
    // 获得 Mapper 映射配置文件对应的 Mapper 接口，实际上类名就是 namespace 。
    String namespace = builderAssistant.getCurrentNamespace();
    if (namespace != null) {
      Class<?> boundType = null;
      try {
        boundType = Resources.classForName(namespace);
      } catch (ClassNotFoundException e) {
        //ignore, bound type is not required
      }
      if (boundType != null) {
        // 不存在该 Mapper 接口，则进行添加
        if (!configuration.hasMapper(boundType)) {
          // Spring may not know the real resource name so we set a flag
          // to prevent loading again this resource from the mapper interface
          // look at MapperAnnotationBuilder#loadXmlResource
          // 标记 namespace 已经添加，避免 MapperAnnotationBuilder#loadXmlResource(...) 重复加载
          configuration.addLoadedResource("namespace:" + namespace);
          // 添加到 configuration 中
          configuration.addMapper(boundType);
        }
      }
    }
  }

}
