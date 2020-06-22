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

import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.AutoMappingUnknownColumnBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  /**
   * 是否已解析
   */
  private boolean parsed;
  /**
   * 基于 Java XPath 解析器
   */
  private final XPathParser parser;
  /**
   * 环境
   */
  private String environment;
  /**
   * ReflectorFactory 对象
   */
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
// <1> 创建 Configuration 对象
    super(new Configuration());
    ErrorContext.instance().resource("SQL Mapper Configuration");
    // <2> 设置 Configuration 的 variables 属性
    this.configuration.setVariables(props);
    this.parsed = false;
    this.environment = environment;
    this.parser = parser;
  }

  /**
   * 解析xml文件
   *
   * @return 将xml中的配置注册到Configuration中
   */
  public Configuration parse() {
    if (parsed) {
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    parsed = true;
    // todo xml核心解析方法
    parseConfiguration(
      // 获取xml根节点对象
      parser.evalNode("/configuration")
    );
    return configuration;
  }

  /**
   * <pre>
   *   <xs:element name="configuration">
   *     <xs:complexType>
   *       <xs:sequence>
   *         <xs:element minOccurs="0" ref="properties"/>
   *         <xs:element minOccurs="0" ref="settings"/>
   *         <xs:element minOccurs="0" ref="typeAliases"/>
   *         <xs:element minOccurs="0" ref="typeHandlers"/>
   *         <xs:element minOccurs="0" ref="objectFactory"/>
   *         <xs:element minOccurs="0" ref="objectWrapperFactory"/>
   *         <xs:element minOccurs="0" ref="reflectorFactory"/>
   *         <xs:element minOccurs="0" ref="plugins"/>
   *         <xs:element minOccurs="0" ref="environments"/>
   *         <xs:element minOccurs="0" ref="databaseIdProvider"/>
   *         <xs:element minOccurs="0" ref="mappers"/>
   *       </xs:sequence>
   *     </xs:complexType>
   *   </xs:element>
   * </pre>
   * 解析xml元素
   *
   * @param root configuration 节点
   */
  private void parseConfiguration(XNode root) {
    try {
      //issue #117 read properties first

      // 1. 解析 <properties /> 标签
      propertiesElement(root.evalNode("properties"));
      // 2. 解析 <settings /> 标签
      Properties settings = settingsAsProperties(root.evalNode("settings"));
      // 3. 加载自定义 VFS 实现类
      loadCustomVfs(settings);
      // 4. 解析 <typeAliases /> 标签
      typeAliasesElement(root.evalNode("typeAliases"));
      // 5. 解析 <plugins /> 标签
      pluginElement(root.evalNode("plugins"));
      // 6. 解析 <objectFactory /> 标签
      objectFactoryElement(root.evalNode("objectFactory"));
      // 7. 解析 <objectWrapperFactory /> 标签
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
      // 8. 解析 <reflectorFactory /> 标签
      reflectorFactoryElement(root.evalNode("reflectorFactory"));
      // 9. 赋值 <settings /> 到 Configuration 属性
      settingsElement(settings);
      // read it after objectFactory and objectWrapperFactory issue #631
      // 10. 解析 <environments /> 标签
      environmentsElement(root.evalNode("environments"));
      // 11. 解析 <databaseIdProvider /> 标签
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));
      // 12. 解析 <typeHandlers /> 标签
      typeHandlerElement(root.evalNode("typeHandlers"));
      // 13. 解析 <mappers /> 标签
      mapperElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  /**
   * <pre>
   * <settings>
   *  <setting name="cacheEnabled" value="true"/>
   *  <setting name="lazyLoadingEnabled" value="true"/>
   *  <setting name="multipleResultSetsEnabled" value="true"/>
   *  <setting name="useColumnLabel" value="true"/>
   *  <setting name="useGeneratedKeys" value="false"/>
   *  <setting name="autoMappingBehavior" value="PARTIAL"/>
   *  <setting name="autoMappingUnknownColumnBehavior" value="WARNING"/>
   *  <setting name="defaultExecutorType" value="SIMPLE"/>
   *  <setting name="defaultStatementTimeout" value="25"/>
   *  <setting name="defaultFetchSize" value="100"/>
   *  <setting name="safeRowBoundsEnabled" value="false"/>
   *  <setting name="mapUnderscoreToCamelCase" value="false"/>
   *  <setting name="localCacheScope" value="SESSION"/>
   *  <setting name="jdbcTypeForNull" value="OTHER"/>
   *  <setting name="lazyLoadTriggerMethods" value="equals,clone,hashCode,toString"/>
   * </settings>
   * </pre>
   * <p>
   * 解析settings标签。
   * settings 是 MyBatis 中极为重要的调整设置，它们会改变 MyBatis 的运行时行为。
   *
   * @param context settings xnode
   * @return 返回验证通过的 setting 的 Properties
   */
  private Properties settingsAsProperties(XNode context) {
    // 如果没有自定义设置settings，那么将返回空Properties
    if (context == null) {
      return new Properties();
    }
    // 获取所有的setting标签，转为Properties
    Properties props = context.getChildrenAsProperties();
    // Check that all settings are known to the configuration class
    // 仅仅验证 在Configuration类中是不是存在这些配置的setting设置属性，如果不存在则报错，否则返回Properties
    // 此步创建Configuration的metaClass，并创建Configuration的Reflector
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
    for (Object key : props.keySet()) {
      // 通过Configuration的Reflector来验证是否存在对应属性。
      if (!metaConfig.hasSetter(String.valueOf(key))) {
        throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    // 返回验证通过的 setting 的 Properties
    return props;
  }

  /**
   * 加载自定义的VFS实现
   *
   * @param props 从 settings中解析出的setting设置
   * @throws ClassNotFoundException e
   */
  private void loadCustomVfs(Properties props) throws ClassNotFoundException {
    // 获取 vfsImpl 属性的值（vfsImpl：自定义 VFS 的实现的类全限定名，以逗号分隔。）
    String value = props.getProperty("vfsImpl");
    // 如果存在vfsImpl的实现
    if (value != null) {
      String[] clazzes = value.split(",");
      for (String clazz : clazzes) {
        if (!clazz.isEmpty()) {
          @SuppressWarnings("unchecked")
          // 根据类名，加载class，然后设置到Configuration中
            Class<? extends VFS> vfsImpl = (Class<? extends VFS>) Resources.classForName(clazz);
          configuration.setVfsImpl(vfsImpl);
        }
      }
    }
  }

  private void loadCustomLogImpl(Properties props) {
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
    configuration.setLogImpl(logImpl);
  }

  /**
   * - 指定类情况：直接使用alias为指定类的别名，如果没有设置别名，将使用类名的首字母小写形式为别名。
   * <pre>
   * <typeAliases>
   *  <typeAlias alias="Author" type="domain.blog.Author"/>
   *  <typeAlias alias="Blog" type="domain.blog.Blog"/>
   *  <typeAlias alias="Comment" type="domain.blog.Comment"/>
   *  <typeAlias alias="Post" type="domain.blog.Post"/>
   *  <typeAlias alias="Section" type="domain.blog.Section"/>
   *  <typeAlias alias="Tag" type="domain.blog.Tag"/>
   * </typeAliases>
   * </pre>
   * <p>
   * - 指定包情况：每一个在包 domain.blog 中的 Java Bean，在没有注解的情况下，
   * 会使用 Bean 的首字母小写的非限定类名来作为它的别名。
   * 比如 domain.blog.Author 的别名为 author
   * <typeAliases>
   * <package name="domain.blog"/>
   * </typeAliases>
   * <p>
   * 解析：类型别名（typeAliases）
   * 类型别名可为 Java 类型设置一个缩写名字。 它仅用于 XML 配置，意在降低冗余的全限定类名书写。
   *
   * @param parent typeAliases节点
   */
  private void typeAliasesElement(XNode parent) {
    if (parent != null) {
      // 遍历所有的 typeAlias 设置，并处理
      for (XNode child : parent.getChildren()) {
        // 指定为包的情况下，注册包下的每个类
        if ("package".equals(child.getName())) {
          String typeAliasPackage = child.getStringAttribute("name");
          configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
        } else {
          // 指定为类的情况下，直接注册类和别名
          String alias = child.getStringAttribute("alias");
          String type = child.getStringAttribute("type");
          try {
            // 获得类是否存在
            Class<?> clazz = Resources.classForName(type);
            if (alias == null) {
              // typeAliasRegistry 中注册别名
              typeAliasRegistry.registerAlias(clazz);
            } else {
              // typeAliasRegistry 中注册别名，使用自定义的别名
              typeAliasRegistry.registerAlias(alias, clazz);
            }
          } catch (ClassNotFoundException e) {
            throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
          }
        }
      }
    }
  }

  /**
   * MyBatis 允许你在映射语句执行过程中的某一点进行拦截调用。默认情况下，MyBatis 允许使用插件来拦截的方法调用包括：
   * - Executor (update, query, flushStatements, commit, rollback, getTransaction, close, isClosed)
   * - ParameterHandler (getParameterObject, setParameters)
   * - ResultSetHandler (handleResultSets, handleOutputParameters)
   * - StatementHandler (prepare, parameterize, batch, update, query)
   *
   * <pre>
   * @Intercepts({@Signature(
   *   type= Executor.class,
   *   method = "update",
   *   args = {MappedStatement.class,Object.class}
   * )})
   * public class ExamplePlugin implements Interceptor {
   *   private Properties properties = new Properties();
   *   public Object intercept(Invocation invocation) throws Throwable {
   *     // implement pre processing if need
   *     Object returnObject = invocation.proceed();
   *     // implement post processing if need
   *     return returnObject;
   *   }
   *   public void setProperties(Properties properties) {
   *     this.properties = properties;
   *   }
   * }
   * <plugins>
   *  <plugin interceptor="org.mybatis.example.ExamplePlugin">
   *    <property name="someProperty" value="100"/>
   *  </plugin>
   * </plugins>
   * 上面的插件将会拦截在 Executor 实例中所有的 “update” 方法调用， 这里的 Executor 是负责执行底层映射语句的内部对象。
   * </pre>
   * <p>
   * 插件节点解析
   *
   * @param parent plugins节点
   * @throws Exception e
   */
  private void pluginElement(XNode parent) throws Exception {
    if (parent != null) {
      // 遍历plugin节点
      for (XNode child : parent.getChildren()) {
        // 获取连接器
        String interceptor = child.getStringAttribute("interceptor");
        // 获取定义的拦截器的Properties对象
        Properties properties = child.getChildrenAsProperties();
        // 实例化Interceptor对象
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).getDeclaredConstructor().newInstance();
        // 谁知Interceptor对象的Properties属性
        interceptorInstance.setProperties(properties);
        // 在configuration中注册InterceptorChain中
        configuration.addInterceptor(interceptorInstance);
      }
    }
  }

  /**
   * ObjectFactory 接口很简单，它包含两个创建实例用的方法，
   * 一个是处理默认无参构造方法的，另外一个是处理带参数的构造方法的。
   * 另外，setProperties 方法可以被用来配置 ObjectFactory，在初始化你的 ObjectFactory 实例后，
   * objectFactory 元素体中定义的属性会被传递给 setProperties 方法。
   *
   * <pre>
   * public class ExampleObjectFactory extends DefaultObjectFactory {
   *   public Object create(Class type) {
   *     return super.create(type);
   *   }
   *   public Object create(Class type, List<Class> constructorArgTypes, List<Object> constructorArgs) {
   *     return super.create(type, constructorArgTypes, constructorArgs);
   *   }
   *   public void setProperties(Properties properties) {
   *     super.setProperties(properties);
   *   }
   *   public <T> boolean isCollection(Class<T> type) {
   *     return Collection.class.isAssignableFrom(type);
   *   }
   * }
   *
   * <!-- mybatis-config.xml -->
   * <objectFactory type="org.mybatis.example.ExampleObjectFactory">
   *   <property name="someProperty" value="100"/>
   * </objectFactory>
   * </pre>
   * <p>
   * 解析 对象工厂节点（objectFactory）
   *
   * @param context objectFactory 节点
   * @throws Exception e
   */
  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      // 获取工厂的自定义实现类，例如：org.mybatis.example.ExampleObjectFactory
      String type = context.getStringAttribute("type");
      // 获取自定义工厂类的Properties
      Properties properties = context.getChildrenAsProperties();
      // 实例化对象工厂
      ObjectFactory factory = (ObjectFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      // 设置对象工厂的properties
      factory.setProperties(properties);
      // 将对象工厂配置到Configuration中
      configuration.setObjectFactory(factory);
    }
  }

  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      // 获取类名称
      String type = context.getStringAttribute("type");
      // 实例化
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      // 注册到Configuration中
      configuration.setObjectWrapperFactory(factory);
    }
  }

  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      // 获取类名称
      String type = context.getStringAttribute("type");
      // 实例化
      ReflectorFactory factory = (ReflectorFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      // 注册到Configuration中
      configuration.setReflectorFactory(factory);
    }
  }

  /**
   * <pre>
   *  <properties resource="org/mybatis/example/config.properties">
   *    <property name="username" value="dev_user"/>
   *    <property name="password" value="F2Fa3!33TYyg"/>
   *  </properties>
   * </pre>
   *
   * <p>
   * properties 标签解析
   *
   * @param context peoperties 节点
   * @throws Exception
   */
  private void propertiesElement(XNode context) throws Exception {
    if (context != null) {
      // 把子节点解析为Properties对象
      Properties defaults = context.getChildrenAsProperties();
      // 获取properties标签属性：resource
      String resource = context.getStringAttribute("resource");
      // 获取properties标签属性：url
      String url = context.getStringAttribute("url");
      if (resource != null && url != null) {
        throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
      }
      if (resource != null) {
        // 如果resource不为空，则获取resource对应的properties文件加载到当前的properties对象defaults中
        defaults.putAll(Resources.getResourceAsProperties(resource));
      } else if (url != null) {
        // 如果url不为空，则获取resource对应的properties文件加载到当前的properties对象defaults中
        defaults.putAll(Resources.getUrlAsProperties(url));
      }
      // 获取在创建 XMLConfigBuilder时在构造器中对 configuration 对象设置的 properties 属性
      Properties vars = configuration.getVariables();
      // 如果不为空，则一并添加到 defaults中
      if (vars != null) {
        defaults.putAll(vars);
      }
      // 设置 parser 中 properties
      parser.setVariables(defaults);
      // 设置 configuration 中 properties
      configuration.setVariables(defaults);
    }
  }

  /**
   * 将解析的settings赋值到 Configuration 中
   * {@link #settingsElement(Properties)}
   *
   * @param props setting properties
   */
  private void settingsElement(Properties props) {
    configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    configuration.setDefaultResultSetType(resolveResultSetType(props.getProperty("defaultResultSetType")));
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
  }

  /**
   * MyBatis 可以配置成适应多种环境，这种机制有助于将 SQL 映射应用于多种数据库之中，
   * 现实情况下有多种理由需要这么做。例如，开发、测试和生产环境需要有不同的配置；
   * <p>
   * 尽管可以配置多个环境，但每个 SqlSessionFactory 实例只能选择一种环境。
   * 每个数据库对应一个 SqlSessionFactory 实例
   *
   * <pre>
   * <environments default="development">
   *   <environment id="development">
   *     <transactionManager type="JDBC">
   *       <property name="closeConnection" value="false"/>
   *     </transactionManager>
   *     <dataSource type="POOLED">
   *       <property name="driver" value="${driver}"/>
   *       <property name="url" value="${url}"/>
   *       <property name="username" value="${username}"/>
   *       <property name="password" value="${password}"/>
   *     </dataSource>
   *   </environment>
   * </environments>
   * </pre>
   * <p>
   * 解析Environment标签
   *
   * @param context
   * @throws Exception
   */
  private void environmentsElement(XNode context) throws Exception {
    if (context != null) {
      if (environment == null) {
        environment = context.getStringAttribute("default");
      }
      for (XNode child : context.getChildren()) {
        // 获取环境名称
        String id = child.getStringAttribute("id");
        // 验证id是否与此 Builder中的environment一致
        if (isSpecifiedEnvironment(id)) {
          // 解析 `<transactionManager />` 标签，返回 TransactionFactory 对象
          TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
          // 解析 `<dataSource />` 标签，返回 DataSourceFactory 对象
          DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
          // 使用数据源工厂获取数据源
          DataSource dataSource = dsFactory.getDataSource();
          // 创建数据库环境对象
          Environment.Builder environmentBuilder = new Environment.Builder(id)
            .transactionFactory(txFactory)
            .dataSource(dataSource);
          // 将数据库环境设置到Configuration
          configuration.setEnvironment(environmentBuilder.build());
        }
      }
    }
  }

  /**
   * MyBatis 可以根据不同的数据库厂商执行不同的语句，这种多厂商的支持是基于映射语句中的 databaseId 属性。
   * MyBatis 会加载带有匹配当前数据库 databaseId 属性和所有不带 databaseId 属性的语句。
   * 如果同时找到带有 databaseId 和不带 databaseId 的相同语句，则后者会被舍弃。
   * 为支持多厂商特性，只要像下面这样在 mybatis-config.xml 文件中加入 databaseIdProvider 即可。
   *
   * <pre>
   * <databaseIdProvider type="DB_VENDOR">
   *   <property name="SQL Server" value="sqlserver"/>
   *   <property name="DB2" value="db2"/>
   *   <property name="Oracle" value="oracle" />
   * </databaseIdProvider>
   * </pre>
   * 数据库厂商标识（databaseIdProvider）
   *
   * @param context databaseIdProvider节点
   * @throws Exception e
   */
  private void databaseIdProviderElement(XNode context) throws Exception {
    DatabaseIdProvider databaseIdProvider = null;
    if (context != null) {
      // 获得 DatabaseIdProvider 的类型
      String type = context.getStringAttribute("type");
      // awful patch to keep backward compatibility
      if ("VENDOR".equals(type)) {
        type = "DB_VENDOR";
      }
      Properties properties = context.getChildrenAsProperties();
      // 创建 DatabaseIdProvider 对象，并设置对应的属性
      databaseIdProvider = (DatabaseIdProvider) resolveClass(type).getDeclaredConstructor().newInstance();
      databaseIdProvider.setProperties(properties);
    }
    // 获取数据库的Environment对象，并设置 DatabaseId
    Environment environment = configuration.getEnvironment();
    if (environment != null && databaseIdProvider != null) {
      // 获得对应的 databaseId 编号
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
      configuration.setDatabaseId(databaseId);
    }
  }

  /**
   * 获取事务管理器工厂类
   *
   * @param context transactionManager节点
   * @return TransactionFactory
   * @throws Exception e
   */
  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      // 获取配置的mybatis的事务管理器。在 MyBatis 中有两种类型的事务管理器（也就是 type="[JDBC|MANAGED]"）
      // 获得 TransactionFactory 的类
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      // 创建 TransactionFactory 对象，并设置属性
      TransactionFactory factory = (TransactionFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  /**
   * <pre>
   * <dataSource type="org.myproject.C3P0DataSourceFactory">
   *   <property name="driver" value="org.postgresql.Driver"/>
   *   <property name="url" value="jdbc:postgresql:mydb"/>
   *   <property name="username" value="postgres"/>
   *   <property name="password" value="root"/>
   * </dataSource>
   * </pre>
   * 获取数据源工厂类
   *
   * @param context 数据源配置节点
   * @return 数据源工厂类
   * @throws Exception e
   */
  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      // 数据源类型
      String type = context.getStringAttribute("type");
      // 数据源配置属性
      Properties props = context.getChildrenAsProperties();
      // 数据源工厂
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      // 设置数据源属性
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  /**
   * MyBatis 在设置预处理语句（PreparedStatement）中的参数或从结果集中取出一个值时，
   * 都会用类型处理器将获取到的值以合适的方式转换成 Java 类型。
   * <pre>
   *    @MappedJdbcTypes(JdbcType.VARCHAR)
   *    public class ExampleTypeHandler extends BaseTypeHandler<String> {
   *       // 实现方法
   *    }
   *    <typeHandlers>
   *      <typeHandler handler="org.mybatis.example.ExampleTypeHandler"/>
   *    </typeHandlers>
   * </pre>
   *
   * @param parent
   */
  private void typeHandlerElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          // 1. 如果是 package 标签，则扫描该包
          String typeHandlerPackage = child.getStringAttribute("name");
          typeHandlerRegistry.register(typeHandlerPackage);
        } else {
          // 2. 如果是 typeHandler 标签，则注册该 typeHandler 信息
          // 获得 javaType、jdbcType、handler
          String javaTypeName = child.getStringAttribute("javaType");
          String jdbcTypeName = child.getStringAttribute("jdbcType");
          String handlerTypeName = child.getStringAttribute("handler");
          Class<?> javaTypeClass = resolveClass(javaTypeName);
          JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
          Class<?> typeHandlerClass = resolveClass(handlerTypeName);
          // 注册 typeHandler
          if (javaTypeClass != null) {
            if (jdbcType == null) {
              typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
            } else {
              typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
            }
          } else {
            typeHandlerRegistry.register(typeHandlerClass);
          }
        }
      }
    }
  }

  /**
   * 既然 MyBatis 的行为已经由上述元素配置完了，我们现在就要来定义 SQL 映射语句了。
   * 可以使用相对于类路径的资源引用，或完全限定资源定位符（包括 file:/// 形式的 URL），或类名和包名等。
   *
   * <pre>
   *  <!-- 使用相对于类路径的资源引用 -->
   *  <mappers>
   *    <mapper resource="org/mybatis/builder/AuthorMapper.xml"/>
   *    <mapper resource="org/mybatis/builder/BlogMapper.xml"/>
   *    <mapper resource="org/mybatis/builder/PostMapper.xml"/>
   *  </mappers>
   *  <!-- 使用完全限定资源定位符（URL） -->
   *  <mappers>
   *    <mapper url="file:///var/mappers/AuthorMapper.xml"/>
   *    <mapper url="file:///var/mappers/BlogMapper.xml"/>
   *    <mapper url="file:///var/mappers/PostMapper.xml"/>
   *  </mappers>
   *  <!-- 使用映射器接口实现类的完全限定类名 -->
   *  <mappers>
   *    <mapper class="org.mybatis.builder.AuthorMapper"/>
   *    <mapper class="org.mybatis.builder.BlogMapper"/>
   *    <mapper class="org.mybatis.builder.PostMapper"/>
   *  </mappers>
   *  <!-- 将包内的映射器接口实现全部注册为映射器 -->
   *  <mappers>
   *    <package name="org.mybatis.builder"/>
   *  </mappers>
   * </pre>
   *
   * @param parent mappers 节点
   * @throws Exception e
   */
  private void mapperElement(XNode parent) throws Exception {
    if (parent != null) {
      // 获取mappers子节点
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          // 1. 将包内的映射器接口实现全部注册为映射器
          String mapperPackage = child.getStringAttribute("name");
          configuration.addMappers(mapperPackage);
        } else {
          // 获得 resource、url、class 属性
          String resource = child.getStringAttribute("resource");
          String url = child.getStringAttribute("url");
          String mapperClass = child.getStringAttribute("class");
          if (resource != null && url == null && mapperClass == null) {
            // 2. 使用相对于类路径的资源引用
            ErrorContext.instance().resource(resource);
            // 获得 resource 的 InputStream 对象
            InputStream inputStream = Resources.getResourceAsStream(resource);
            // 创建 XMLMapperBuilder 对象
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
            // 执行解析 todo mybatis-mapper.xml核心解析
            mapperParser.parse();
          } else if (resource == null && url != null && mapperClass == null) {
            // 3. 使用完全限定资源定位符（URL）
            ErrorContext.instance().resource(url);
            // 获得 url 的 InputStream 对象
            InputStream inputStream = Resources.getUrlAsStream(url);
            // 创建 XMLMapperBuilder 对象
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
            // 执行解析
            mapperParser.parse();
          } else if (resource == null && url == null && mapperClass != null) {
            // 4. 使用映射器接口实现类的完全限定类名
            // 获得 Mapper 接口
            Class<?> mapperInterface = Resources.classForName(mapperClass);
            // 添加到 configuration 中
            configuration.addMapper(mapperInterface);
          } else {
            throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
          }
        }
      }
    }
  }

  /**
   * 验证id是否与此 Builder中的environment一致
   *
   * @param id
   * @return
   */
  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    } else if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    } else if (environment.equals(id)) {
      return true;
    }
    return false;
  }

}
