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
package org.apache.ibatis.scripting.xmltags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 继承 BaseBuilder 抽象类，XML 动态语句( SQL )构建器，负责将 SQL 解析成 SqlSource 对象。
 *
 * @author Clinton Begin
 */
public class XMLScriptBuilder extends BaseBuilder {

  /**
   * 当前 SQL 的 XNode 对象
   */
  private final XNode context;
  /**
   * 是否为动态 SQL
   */
  private boolean isDynamic;
  /**
   * SQL 方法类型
   */
  private final Class<?> parameterType;
  /**
   * NodeHandler 的映射
   */
  private final Map<String, NodeHandler> nodeHandlerMap = new HashMap<>();

  public XMLScriptBuilder(Configuration configuration, XNode context) {
    this(configuration, context, null);
  }

  /**
   * 创建 XMLScriptBuilder 对象
   *
   * @param configuration Mybatis 对配置中心
   * @param context       xml的节点信息
   * @param parameterType 参数类型
   */
  public XMLScriptBuilder(Configuration configuration, XNode context, Class<?> parameterType) {
    super(configuration);
    this.context = context;
    this.parameterType = parameterType;
    // 初始化 nodeHandlerMap 属性
    initNodeHandlerMap();
  }


  private void initNodeHandlerMap() {
    // 初始化一些用于处理xml的处理器。
    // nodeHandlerMap 的 KEY 是熟悉的 MyBatis 的自定义的 XML 标签。并且，每个标签对应专属的一个 NodeHandler 实现类。
    nodeHandlerMap.put("trim", new TrimHandler());
    nodeHandlerMap.put("where", new WhereHandler());
    nodeHandlerMap.put("set", new SetHandler());
    nodeHandlerMap.put("foreach", new ForEachHandler());
    nodeHandlerMap.put("if", new IfHandler());
    nodeHandlerMap.put("choose", new ChooseHandler());
    nodeHandlerMap.put("when", new IfHandler());
    nodeHandlerMap.put("otherwise", new OtherwiseHandler());
    nodeHandlerMap.put("bind", new BindHandler());
  }

  /**
   * 负责将 SQL 解析成 SqlSource 对象。
   *
   * @return SqlSource
   */
  public SqlSource parseScriptNode() {
    // 解析 SQL
    MixedSqlNode rootSqlNode = parseDynamicTags(context);
    // 创建 SqlSource 对象
    SqlSource sqlSource;
    if (isDynamic) {
      sqlSource = new DynamicSqlSource(configuration, rootSqlNode);
    } else {
      sqlSource = new RawSqlSource(configuration, rootSqlNode, parameterType);
    }
    return sqlSource;
  }

  /**
   * 解析 SQL 成 MixedSqlNode 对象。
   * <p>
   * todo 此方法的解析流程是怎么样的？？？
   *
   * <pre>
   *  <sql id="FIELDS">
   *      id, username, password, create_time
   *  </sql>
   *
   *  <select id="selectByIds" resultType="UserDO">
   *      SELECT
   *          <include refid="FIELDS" />
   *      FROM users
   *      WHERE id IN
   *          <foreach item="id" collection="ids" separator="," open="(" close=")" index="">
   *              #{id}
   *          </foreach>
   *  </select>
   * </pre>
   *
   * @param node xml的sql节点
   * @return
   */
  protected MixedSqlNode parseDynamicTags(XNode node) {
    // 1. 创建 SqlNode 数组
    List<SqlNode> contents = new ArrayList<>();
    // 2. 遍历 SQL 节点的所有子节点
    NodeList children = node.getNode().getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      // 2.1 当前子节点
      XNode child = node.newXNode(children.item(i));
      // 2.2 如果类型是 Node.CDATA_SECTION_NODE 或者 Node.TEXT_NODE 时
      if (child.getNode().getNodeType() == Node.CDATA_SECTION_NODE ||
        child.getNode().getNodeType() == Node.TEXT_NODE) {
        // 2.2.1 获得内容
        String data = child.getStringBody("");
        // 2.2.2 创建 TextSqlNode 对象
        TextSqlNode textSqlNode = new TextSqlNode(data);
        // 2.2.3 如果是动态的 TextSqlNode 对象
        if (textSqlNode.isDynamic()) {
          // 添加到 contents 中
          contents.add(textSqlNode);
          // 标记为动态 SQL
          isDynamic = true;
          // 2.2.4 如果是非动态的 TextSqlNode 对象
        } else {
          contents.add(new StaticTextSqlNode(data));
        }
        // gcode issue #628
        // 2.3 如果类型是 Node.ELEMENT_NODE
      } else if (child.getNode().getNodeType() == Node.ELEMENT_NODE) {
        // 2.3.1 根据子节点的标签，获得对应的 NodeHandler 对象
        String nodeName = child.getNode().getNodeName();
        NodeHandler handler = nodeHandlerMap.get(nodeName);
        if (handler == null) {
          throw new BuilderException("Unknown element <" + nodeName + "> in SQL statement.");
        }
        // 2.3.2 执行 NodeHandler 处理
        handler.handleNode(child, contents);
        // 2.2.3 标记为动态 SQL
        isDynamic = true;
      }
    }
    // 3. 创建 MixedSqlNode 对象
    return new MixedSqlNode(contents);
  }

  /**
   * 对 XNode 解析的方法统一抽象
   */
  private interface NodeHandler {
    /**
     * 解析 node
     *
     * @param nodeToHandle   需要处理的node
     * @param targetContents 解析的结果存放的容器
     *                       实际上，被处理的 XNode 节点会创建成对应的 SqlNode 对象，添加到 targetContents 中
     */
    void handleNode(XNode nodeToHandle, List<SqlNode> targetContents);
  }

  /**
   * BindHandler ，实现 NodeHandler 接口，<bind /> 标签的处理器。
   * <p>
   * bind 元素允许你在 OGNL 表达式以外创建一个变量，并将其绑定到当前的上下文。
   * <pre>
   *    <select id="selectBlogsLike" resultType="Blog">
   *      <bind name="pattern" value="'%' + _parameter.getTitle() + '%'" />
   *      SELECT * FROM BLOG
   *      WHERE title LIKE #{pattern}
   *    </select>
   * </pre>
   */
  private class BindHandler implements NodeHandler {
    public BindHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      // 获取 name 属性
      final String name = nodeToHandle.getStringAttribute("name");
      // 获取 value 属性
      final String expression = nodeToHandle.getStringAttribute("value");
      // 创建 VarDeclSqlNode 对象
      final VarDeclSqlNode node = new VarDeclSqlNode(name, expression);
      // 添加到 targetContents 中
      targetContents.add(node);
    }
  }

  /**
   * <pre>
   *     <trim prefix="WHERE" prefixOverrides="AND | OR ">
   *       ...
   *     </trim>
   *
   *     <update id="testTrim" parameterType="com.mybatis.pojo.User">
   *         update user
   *         set
   *         <trim suffixOverrides="," suffix="where id = #{id}">
   *             <if test="cash!=null and cash!=''">cash= #{cash},</if>
   *             <if test="address!=null and address!=''">address= #{address},</if>
   *         </trim>
   *     </update>
   * </pre>
   */
  private class TrimHandler implements NodeHandler {
    public TrimHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      // <1> 解析内部的 SQL 节点，成 MixedSqlNode 对象
      MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
      // <2> 获得 prefix、prefixOverrides、"suffix"、suffixOverrides 属性
      String prefix = nodeToHandle.getStringAttribute("prefix");
      String prefixOverrides = nodeToHandle.getStringAttribute("prefixOverrides");
      String suffix = nodeToHandle.getStringAttribute("suffix");
      String suffixOverrides = nodeToHandle.getStringAttribute("suffixOverrides");
      // <3> 创建 TrimSqlNode 对象
      TrimSqlNode trim = new TrimSqlNode(configuration, mixedSqlNode, prefix, prefixOverrides, suffix, suffixOverrides);
      // <4> 添加到 targetContents 中
      targetContents.add(trim);
    }
  }

  /**
   * WhereHandler ，实现 NodeHandler 接口，<where /> 标签的处理器。
   * <pre>
   * <select id="findActiveBlogLike"
   *      resultType="Blog">
   *   SELECT * FROM BLOG
   *   <where>
   *     <if test="state != null">
   *          state = #{state}
   *     </if>
   *     <if test="title != null">
   *         AND title like #{title}
   *     </if>
   *     <if test="author != null and author.name != null">
   *         AND author_name like #{author.name}
   *     </if>
   *   </where>
   * </select>
   * </pre>
   */
  private class WhereHandler implements NodeHandler {
    public WhereHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      // 解析内部的 SQL 节点，成 MixedSqlNode 对象
      MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
      // 创建 WhereSqlNode 对象
      WhereSqlNode where = new WhereSqlNode(configuration, mixedSqlNode);
      // 添加到 targetContents 中
      targetContents.add(where);
    }
  }

  /**
   * SetHandler ，实现 NodeHandler 接口，<set /> 标签的处理器。
   * <pre>
   * <update id="updateAuthorIfNecessary">
   *   update Author
   *     <set>
   *       <if test="username != null">username=#{username},</if>
   *       <if test="password != null">password=#{password},</if>
   *       <if test="email != null">email=#{email},</if>
   *       <if test="bio != null">bio=#{bio}</if>
   *     </set>
   *   where id=#{id}
   * </update>
   * </pre>
   */
  private class SetHandler implements NodeHandler {
    public SetHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      // 解析内部的 SQL 节点，成 MixedSqlNode 对象
      MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
      // 创建 SetSqlNode 对象
      SetSqlNode set = new SetSqlNode(configuration, mixedSqlNode);
      // 添加到 targetContents 中
      targetContents.add(set);
    }
  }

  /**
   * ForEachHandler ，实现 NodeHandler 接口，<foreach /> 标签的处理器。
   * <pre>
   * <select id="selectPostIn" resultType="domain.blog.Post">
   *   SELECT *
   *   FROM POST P
   *   WHERE ID in
   *   <foreach item="item" index="index" collection="list" open="(" separator="," close=")">
   *         #{item}
   *   </foreach>
   * </select>
   * </pre>
   */
  private class ForEachHandler implements NodeHandler {
    public ForEachHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      // 解析内部的 SQL 节点，成 MixedSqlNode 对象
      MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
      // 获得 collection、item、index、open、close、separator 属性
      String collection = nodeToHandle.getStringAttribute("collection");
      String item = nodeToHandle.getStringAttribute("item");
      String index = nodeToHandle.getStringAttribute("index");
      String open = nodeToHandle.getStringAttribute("open");
      String close = nodeToHandle.getStringAttribute("close");
      String separator = nodeToHandle.getStringAttribute("separator");
      // 创建 ForEachSqlNode 对象
      ForEachSqlNode forEachSqlNode = new ForEachSqlNode(configuration, mixedSqlNode, collection, index, item, open, close, separator);
      // 添加到 targetContents 中
      targetContents.add(forEachSqlNode);
    }
  }

  /**
   * IfHandler ，实现 NodeHandler 接口，<if /> 标签的处理器。
   * <pre>
   * <select id="findActiveBlogLike"
   *      resultType="Blog">
   *   SELECT * FROM BLOG WHERE state = ‘ACTIVE’
   *   <if test="title != null">
   *     AND title like #{title}
   *   </if>
   *   <if test="author != null and author.name != null">
   *     AND author_name like #{author.name}
   *   </if>
   * </select>
   * </pre>
   */
  private class IfHandler implements NodeHandler {
    public IfHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      // 解析内部的 SQL 节点，成 MixedSqlNode 对象
      MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
      // 获得 test 属性
      String test = nodeToHandle.getStringAttribute("test");
      // 创建 IfSqlNode 对象
      IfSqlNode ifSqlNode = new IfSqlNode(mixedSqlNode, test);
      // 添加到 targetContents 中
      targetContents.add(ifSqlNode);
    }
  }

  /**
   * OtherwiseHandler ，实现 NodeHandler 接口，<otherwise /> 标签的处理器。
   * <pre>
   * <select id="findActiveBlogLike"
   *      resultType="Blog">
   *   SELECT * FROM BLOG WHERE state = ‘ACTIVE’
   *   <choose>
   *     <when test="title != null">
   *       AND title like #{title}
   *     </when>
   *     <when test="author != null and author.name != null">
   *       AND author_name like #{author.name}
   *     </when>
   *     <otherwise>
   *       AND featured = 1
   *     </otherwise>
   *   </choose>
   * </select>
   * </pre>
   */
  private class OtherwiseHandler implements NodeHandler {
    public OtherwiseHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      // 解析内部的 SQL 节点，成 MixedSqlNode 对象
      MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
      // 添加到 targetContents 中
      targetContents.add(mixedSqlNode);
    }
  }

  /**
   * ChooseHandler ，实现 NodeHandler 接口，<choose /> 标签的处理器。
   *
   * <pre>
   * <select id="findActiveBlogLike"
   *      resultType="Blog">
   *   SELECT * FROM BLOG WHERE state = ‘ACTIVE’
   *   <choose>
   *     <when test="title != null">
   *       AND title like #{title}
   *     </when>
   *     <when test="author != null and author.name != null">
   *       AND author_name like #{author.name}
   *     </when>
   *     <otherwise>
   *       AND featured = 1
   *     </otherwise>
   *   </choose>
   * </select>
   * </pre>
   */
  private class ChooseHandler implements NodeHandler {
    public ChooseHandler() {
      // Prevent Synthetic Access
    }

    @Override
    public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
      List<SqlNode> whenSqlNodes = new ArrayList<>();
      List<SqlNode> otherwiseSqlNodes = new ArrayList<>();
      // 解析 `<when />` 和 `<otherwise />` 的节点们
      handleWhenOtherwiseNodes(nodeToHandle, whenSqlNodes, otherwiseSqlNodes);
      // 获得 `<otherwise />` 的节点
      SqlNode defaultSqlNode = getDefaultSqlNode(otherwiseSqlNodes);
      // 创建 ChooseSqlNode 对象
      ChooseSqlNode chooseSqlNode = new ChooseSqlNode(whenSqlNodes, defaultSqlNode);
      // 添加到 targetContents 中
      targetContents.add(chooseSqlNode);
    }

    /**
     * 处理 when | otherWise 节点，并将处理结果添加到 defaultSqlNodes 中
     *
     * @param chooseSqlNode   chooseSqlNode 节点
     * @param ifSqlNodes      ifSqlNode 节点
     * @param defaultSqlNodes chooseSqlNode 的子节点容器
     */
    private void handleWhenOtherwiseNodes(XNode chooseSqlNode, List<SqlNode> ifSqlNodes, List<SqlNode> defaultSqlNodes) {
      List<XNode> children = chooseSqlNode.getChildren();
      for (XNode child : children) {
        String nodeName = child.getNode().getNodeName();
        NodeHandler handler = nodeHandlerMap.get(nodeName);
        // 处理 `<when />` 节点
        if (handler instanceof IfHandler) {
          handler.handleNode(child, ifSqlNodes);
          // 处理 `<otherwise />` 标签的情况
        } else if (handler instanceof OtherwiseHandler) {
          handler.handleNode(child, defaultSqlNodes);
        }
      }
    }

    /**
     * 至多允许有一个 SqlNode 节点
     *
     * @param defaultSqlNodes
     * @return
     */
    private SqlNode getDefaultSqlNode(List<SqlNode> defaultSqlNodes) {
      SqlNode defaultSqlNode = null;
      if (defaultSqlNodes.size() == 1) {
        defaultSqlNode = defaultSqlNodes.get(0);
      } else if (defaultSqlNodes.size() > 1) {
        throw new BuilderException("Too many default (otherwise) elements in choose statement.");
      }
      return defaultSqlNode;
    }
  }

}
