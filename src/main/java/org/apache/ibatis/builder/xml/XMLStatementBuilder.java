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
package org.apache.ibatis.builder.xml;

import java.util.List;
import java.util.Locale;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;

/**
 * 每一个 <select />、<insert />、<update />、<delete /> 标签，对应一个XMLStatementBuilder
 *
 * @author Clinton Begin
 */
public class XMLStatementBuilder extends BaseBuilder {

  /**
   * mapperBuilderAssistant
   */
  private final MapperBuilderAssistant builderAssistant;
  /**
   * 当前 XML 节点，例如：<select />、<insert />、<update />、<delete /> 标签
   */
  private final XNode context;
  /**
   * 要求的 databaseId
   */
  private final String requiredDatabaseId;

  public XMLStatementBuilder(Configuration configuration, MapperBuilderAssistant builderAssistant, XNode context) {
    this(configuration, builderAssistant, context, null);
  }

  public XMLStatementBuilder(Configuration configuration, MapperBuilderAssistant builderAssistant, XNode context, String databaseId) {
    super(configuration);
    this.builderAssistant = builderAssistant;
    this.context = context;
    this.requiredDatabaseId = databaseId;
  }

  /**
   * 解析 Statement 配置，即 <select />、<insert />、<update />、<delete /> 标签。
   *
   * <pre>
   * <insert
   *   id="insertAuthor"
   *   parameterType="domain.blog.Author"
   *   flushCache="true"
   *   statementType="PREPARED"
   *   keyProperty=""         -> （仅适用于 insert 和 update）指定能够唯一识别对象的属性，MyBatis 会使用 getGeneratedKeys 的返回值或 insert 语句的 selectKey 子元素设置它的值，默认值：未设置（unset）。
   *   keyColumn=""           -> （仅适用于 insert 和 update）设置生成键值在表中的列名，在某些数据库（像 PostgreSQL）中，当主键列不是表中的第一列的时候，是必须设置的。
   *   useGeneratedKeys=""    -> （仅适用于 insert 和 update）这会令 MyBatis 使用 JDBC 的 getGeneratedKeys 方法来取出由数据库内部生成的主键
   *   timeout="20">
   *
   * <delete
   *   id="deleteAuthor"
   *   parameterType="domain.blog.Author"
   *   flushCache="true"
   *   statementType="PREPARED"
   *   timeout="20">
   *
   * <update
   *   id="updateAuthor"
   *   parameterType="domain.blog.Author"
   *   flushCache="true"
   *   statementType="PREPARED"
   *   timeout="20">
   *
   * <select
   *   id="selectPerson"          -> 在命名空间中唯一的标识符，可以被用来引用这条语句。
   *   parameterType="int"        -> 将会传入这条语句的参数的类全限定名或别名。
   *   parameterMap="deprecated"
   *   resultType="hashmap"
   *   resultMap="personResultMap"
   *   flushCache="false"         -> 将其设置为 true 后，只要语句被调用，都会导致本地缓存和二级缓存被清空，默认值：（对 insert、update 和 delete 语句）true。
   *   useCache="true"
   *   timeout="10"               -> 这个设置是在抛出异常之前，驱动程序等待数据库返回请求结果的秒数。默认值为未设置（unset）
   *   fetchSize="256"            -> 这是一个给驱动的建议值，尝试让驱动程序每次批量返回的结果行数等于这个设置值。
   *   statementType="PREPARED"   -> 可选 STATEMENT，PREPARED 或 CALLABLE。这会让 MyBatis 分别使用 Statement，PreparedStatement 或 CallableStatement，默认值：PREPARED。
   *   resultSetType="FORWARD_ONLY"
   *   databaseId="">             -> 如果配置了数据库厂商标识（databaseIdProvider），MyBatis 会加载所有不带 databaseId 或匹配当前 databaseId 的语句；如果带和不带的语句都有，则不带的会被忽略。
   *
   * </pre>
   */
  public void parseStatementNode() {
    // 获取 sql 的 ID
    String id = context.getStringAttribute("id");
    // 获取 databaseId
    String databaseId = context.getStringAttribute("databaseId");

    // 如果 databaseId 不一致则结束处理
    if (!databaseIdMatchesCurrent(id, databaseId, this.requiredDatabaseId)) {
      return;
    }

    // 获取 node 的名称：select、insert、update、delete、flush
    String nodeName = context.getNode().getNodeName();
    // 将sql类型转为枚举类
    SqlCommandType sqlCommandType = SqlCommandType.valueOf(nodeName.toUpperCase(Locale.ENGLISH));
    // 是否为 select 语句
    boolean isSelect = sqlCommandType == SqlCommandType.SELECT;
    // 非 select 语句，则 flushCache = false
    boolean flushCache = context.getBooleanAttribute("flushCache", !isSelect);
    // select 语句则 useCache = true
    boolean useCache = context.getBooleanAttribute("useCache", isSelect);
    boolean resultOrdered = context.getBooleanAttribute("resultOrdered", false);

    // Include Fragments before parsing
    // 创建 XMLIncludeTransformer 对象，并替换 <include /> 标签相关的内容
    /*
    <sql id="userColumns"> ${alias}.id,${alias}.username,${alias}.password </sql>

    <select id="selectUsers" resultType="map">
     select
       <include refid="userColumns"><property name="alias" value="t1"/></include>,
       <include refid="userColumns"><property name="alias" value="t2"/></include>
     from some_table t1
       cross join some_table t2
    </select>
     */
    XMLIncludeTransformer includeParser = new XMLIncludeTransformer(configuration, builderAssistant);
    // 替换<include></include>标签文本
    includeParser.applyIncludes(context.getNode());

    // 获取 parameterType，及 class
    String parameterType = context.getStringAttribute("parameterType");
    Class<?> parameterTypeClass = resolveClass(parameterType);

    // 获取 lang 属性
    /* 动态 SQL 中的插入脚本语言
      MyBatis 从 3.2 版本开始支持插入脚本语言，这允许你插入一种语言驱动，并基于这种语言来编写动态 SQL 查询语句。
      可以通过实现以下接口来插入一种语言：
    public interface LanguageDriver {
      ParameterHandler createParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql);
      SqlSource createSqlSource(Configuration configuration, XNode script, Class<?> parameterType);
      SqlSource createSqlSource(Configuration configuration, String script, Class<?> parameterType);
    }

    实现自定义语言驱动后，你就可以在 mybatis-config.xml 文件中将它设置为默认语言：
    <typeAliases>
      <typeAlias type="org.sample.MyLanguageDriver" alias="myLanguage"/>
    </typeAliases>
    <settings>
      <setting name="defaultScriptingLanguage" value="myLanguage"/>
    </settings>

    或者，你也可以使用 lang 属性为特定的语句指定语言：
    <select id="selectBlog" lang="myLanguage">
      SELECT * FROM BLOG
    </select>

    或者，在你的 mapper 接口上添加 @Lang 注解：
    public interface Mapper {
      @Lang(MyLanguageDriver.class)
      @Select("SELECT * FROM BLOG")
      List<Blog> selectBlog();
    }
     */
    String lang = context.getStringAttribute("lang");
    LanguageDriver langDriver = getLanguageDriver(lang);

    // Parse selectKey after includes and remove them.
    // 解析 selectKey 属性
    /* selectKey
      <selectKey
      keyProperty="id"          -> selectKey 语句结果应该被设置到的目标属性。如果生成列不止一个，可以用逗号分隔多个属性名称。
      resultType="int"          -> 结果的类型。
      order="BEFORE"            -> 可以设置为 BEFORE 或 AFTER。如果设置为 BEFORE，那么它首先会生成主键，设置 keyProperty 再执行插入语句。
      statementType="PREPARED"  -> MyBatis 支持 STATEMENT，PREPARED 和 CALLABLE 类型的映射语句，分别代表 Statement, PreparedStatement 和 CallableStatement 类型。
      >

      <insert id="insertAuthor">
        <selectKey keyProperty="id" resultType="int" order="BEFORE">
          select CAST(RANDOM()*1000000 as INTEGER) a from SYSIBM.SYSDUMMY1
        </selectKey>
        insert into Author
          (id, username, password, email,bio, favourite_section)
        values
          (#{id}, #{username}, #{password}, #{email}, #{bio}, #{favouriteSection,jdbcType=VARCHAR})
      </insert>
      首先会运行 selectKey 元素中的语句，并设置 Author 的 id，然后才会调用插入语句。
     */
    processSelectKeyNodes(id, parameterTypeClass, langDriver);

    // Parse the SQL (pre: <selectKey> and <include> were parsed and removed)
    // 获得 KeyGenerator 对象
    KeyGenerator keyGenerator;
    // 优先，从 configuration 中获得 KeyGenerator 对象。如果存在，意味着是 <selectKey /> 标签配置的
    String keyStatementId = id + SelectKeyGenerator.SELECT_KEY_SUFFIX;
    keyStatementId = builderAssistant.applyCurrentNamespace(keyStatementId, true);
    if (configuration.hasKeyGenerator(keyStatementId)) {
      keyGenerator = configuration.getKeyGenerator(keyStatementId);
    } else {
      // 其次，根据标签属性的情况，判断是否使用对应的 Jdbc3KeyGenerator 或者 NoKeyGenerator 对象
      keyGenerator = context.getBooleanAttribute("useGeneratedKeys",
        configuration.isUseGeneratedKeys() && SqlCommandType.INSERT.equals(sqlCommandType))
        ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
    }

    // 创建 SqlSource
    SqlSource sqlSource = langDriver.createSqlSource(configuration, context, parameterTypeClass);
    // 获取属性
    StatementType statementType = StatementType.valueOf(context.getStringAttribute("statementType", StatementType.PREPARED.toString()));
    Integer fetchSize = context.getIntAttribute("fetchSize");
    Integer timeout = context.getIntAttribute("timeout");
    String parameterMap = context.getStringAttribute("parameterMap");
    String resultType = context.getStringAttribute("resultType");
    Class<?> resultTypeClass = resolveClass(resultType);
    String resultMap = context.getStringAttribute("resultMap");
    String resultSetType = context.getStringAttribute("resultSetType");
    ResultSetType resultSetTypeEnum = resolveResultSetType(resultSetType);
    if (resultSetTypeEnum == null) {
      resultSetTypeEnum = configuration.getDefaultResultSetType();
    }
    String keyProperty = context.getStringAttribute("keyProperty");
    String keyColumn = context.getStringAttribute("keyColumn");
    String resultSets = context.getStringAttribute("resultSets");

    // 创建 MappedStatement 对象，并添加到 Configuration
    builderAssistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType,
      fetchSize, timeout, parameterMap, parameterTypeClass, resultMap, resultTypeClass,
      resultSetTypeEnum, flushCache, useCache, resultOrdered,
      keyGenerator, keyProperty, keyColumn, databaseId, langDriver, resultSets);
  }

  /**
   * 获取当前节点下的所有 selectKey 节点
   *
   * @param id                 当前SQL的id
   * @param parameterTypeClass 当前SQL配置的 parameterType
   * @param langDriver         当前节点配置的lang属性
   */
  private void processSelectKeyNodes(String id, Class<?> parameterTypeClass, LanguageDriver langDriver) {
    // 通过 XPath 语法获取 selectKey 节点
    List<XNode> selectKeyNodes = context.evalNodes("selectKey");
    if (configuration.getDatabaseId() != null) {
      parseSelectKeyNodes(id, selectKeyNodes, parameterTypeClass, langDriver, configuration.getDatabaseId());
    }
    parseSelectKeyNodes(id, selectKeyNodes, parameterTypeClass, langDriver, null);
    removeSelectKeyNodes(selectKeyNodes);
  }

  /**
   * 遍历所有 selectKey 节点，并处理
   *
   * @param parentId             当前SQL的id
   * @param list                 selectKey 节点 list
   * @param parameterTypeClass   当前SQL配置的 parameterType
   * @param langDriver           当前节点配置的lang属性
   * @param skRequiredDatabaseId 是否需要databaseI
   */
  private void parseSelectKeyNodes(String parentId, List<XNode> list, Class<?> parameterTypeClass, LanguageDriver langDriver, String skRequiredDatabaseId) {
    for (XNode nodeToHandle : list) {
      // 获得完整 id ，格式为 `${id}!selectKey`
      String id = parentId + SelectKeyGenerator.SELECT_KEY_SUFFIX;
      // 获得 databaseId ， 判断 databaseId 是否匹配
      String databaseId = nodeToHandle.getStringAttribute("databaseId");
      if (databaseIdMatchesCurrent(id, databaseId, skRequiredDatabaseId)) {
        // 执行解析单个 <selectKey /> 节点
        parseSelectKeyNode(id, nodeToHandle, parameterTypeClass, langDriver, databaseId);
      }
    }
  }

  /**
   * 执行解析单个 <selectKey /> 节点。
   * <pre>
   *     <selectKey
   *     keyProperty="id"          -> selectKey 语句结果应该被设置到的目标属性。如果生成列不止一个，可以用逗号分隔多个属性名称。
   *     resultType="int"          -> 结果的类型。
   *     order="BEFORE"            -> 可以设置为 BEFORE 或 AFTER。如果设置为 BEFORE，那么它首先会生成主键，设置 keyProperty 再执行插入语句。
   *     statementType="PREPARED">  -> MyBatis 支持 STATEMENT，PREPARED 和 CALLABLE 类型的映射语句，分别代表 Statement, PreparedStatement 和 CallableStatement 类型。
   *
   *     <insert id="insertAuthor">
   *       <selectKey keyProperty="id" resultType="int" order="BEFORE">
   *         select CAST(RANDOM()*1000000 as INTEGER) a from SYSIBM.SYSDUMMY1
   *       </selectKey>
   *       insert into Author
   *         (id, username, password, email,bio, favourite_section)
   *       values
   *         (#{id}, #{username}, #{password}, #{email}, #{bio}, #{favouriteSection,jdbcType=VARCHAR})
   *     </insert>
   *     首先会运行 selectKey 元素中的语句，并设置 Author 的 id，然后才会调用插入语句。
   * </pre>
   * <p>
   * 首先读取selectkey中配置的一系列属性，然后调用 {@link LanguageDriver#createSqlSource}来创建SqlSource对象。
   * 最后创建MapperStatement对象，并添加到 {@link Configuration##mappedStatements}
   *
   * @param id                 完整 id ，格式为 `${id}!selectKey`
   * @param nodeToHandle       需要处理的selectKey节点
   * @param parameterTypeClass 当前SQL配置的 parameterType
   * @param langDriver         当前节点配置的lang属性
   * @param databaseId         databaseId
   */
  private void parseSelectKeyNode(String id, XNode nodeToHandle, Class<?> parameterTypeClass, LanguageDriver langDriver, String databaseId) {
    // 获取selectKey的类型，及class
    String resultType = nodeToHandle.getStringAttribute("resultType");
    Class<?> resultTypeClass = resolveClass(resultType);
    // 获取selectKey 的 statementType，默认为：StatementType.PREPARED
    StatementType statementType = StatementType.valueOf(nodeToHandle.getStringAttribute("statementType", StatementType.PREPARED.toString()));
    // selectKey 语句结果应该被设置到的目标属性。如果生成列不止一个，可以用逗号分隔多个属性名称。
    String keyProperty = nodeToHandle.getStringAttribute("keyProperty");
    // 返回结果集中生成列属性的列名。如果生成列不止一个，可以用逗号分隔多个属性名称。
    String keyColumn = nodeToHandle.getStringAttribute("keyColumn");
    boolean executeBefore = "BEFORE".equals(nodeToHandle.getStringAttribute("order", "AFTER"));

    //defaults
    // 创建 MappedStatement 需要用到的默认值
    boolean useCache = false;
    boolean resultOrdered = false;
    KeyGenerator keyGenerator = NoKeyGenerator.INSTANCE;
    Integer fetchSize = null;
    Integer timeout = null;
    boolean flushCache = false;
    String parameterMap = null;
    String resultMap = null;
    ResultSetType resultSetTypeEnum = null;

    // 创建 SqlSource 对象
    SqlSource sqlSource = langDriver.createSqlSource(configuration, nodeToHandle, parameterTypeClass);
    SqlCommandType sqlCommandType = SqlCommandType.SELECT;

    // 创建 MappedStatement 对象
    builderAssistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType,
      fetchSize, timeout, parameterMap, parameterTypeClass, resultMap, resultTypeClass,
      resultSetTypeEnum, flushCache, useCache, resultOrdered,
      keyGenerator, keyProperty, keyColumn, databaseId, langDriver, null);

    // 获得 SelectKeyGenerator 的编号，格式为 `${namespace}.${id}`
    id = builderAssistant.applyCurrentNamespace(id, false);
    // 获得 MappedStatement 对象
    MappedStatement keyStatement = configuration.getMappedStatement(id, false);
    // 创建 SelectKeyGenerator 对象，并添加到 configuration 中
    configuration.addKeyGenerator(id, new SelectKeyGenerator(keyStatement, executeBefore));
  }

  private void removeSelectKeyNodes(List<XNode> selectKeyNodes) {
    for (XNode nodeToHandle : selectKeyNodes) {
      nodeToHandle.getParent().getNode().removeChild(nodeToHandle.getNode());
    }
  }

  private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
    if (requiredDatabaseId != null) {
      return requiredDatabaseId.equals(databaseId);
    }
    if (databaseId != null) {
      return false;
    }
    id = builderAssistant.applyCurrentNamespace(id, false);
    if (!this.configuration.hasStatement(id, false)) {
      return true;
    }
    // skip this statement if there is a previous one with a not null databaseId
    // issue #2
    MappedStatement previous = this.configuration.getMappedStatement(id, false);
    return previous.getDatabaseId() == null;
  }

  /**
   * @param lang
   * @return
   */
  private LanguageDriver getLanguageDriver(String lang) {
    Class<? extends LanguageDriver> langClass = null;
    if (lang != null) {
      langClass = resolveClass(lang);
    }
    return configuration.getLanguageDriver(langClass);
  }

}
