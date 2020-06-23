/**
 * Copyright 2009-2015 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.apache.ibatis.session.Configuration;

/**
 * 继承 TrimSqlNode 类，<where /> 标签的 SqlNode 实现类。
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
 * <p>
 * 为什么需要继承TrimSqlNode？
 * Where条件以 WHERE 字段作为前缀 prefix 字段，prefixToOverride 集合中的项为 AND 或者 OR 。
 * 也就是说，<where> 节点解析之后的SQL语句片段，如果已 AND 或者 OR 开头，
 * 那么将开头处的 AND 或 OR 删除后，再将 WHERE 关键字添加到SQL片段开始位置，从而到到该 <where> 节点最终生成的SQL片段。
 *
 * @author Clinton Begin
 */
public class WhereSqlNode extends TrimSqlNode {

  private static List<String> prefixList = Arrays.asList("AND ", "OR ", "AND\n", "OR\n", "AND\r", "OR\r", "AND\t", "OR\t");

  public WhereSqlNode(Configuration configuration, SqlNode contents) {
    super(configuration, contents, "WHERE", prefixList, null, null);
  }

}
