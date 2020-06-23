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
package org.apache.ibatis.scripting.xmltags;

import java.util.Collections;
import java.util.List;

import org.apache.ibatis.session.Configuration;

/**
 * 继承 TrimSqlNode 类，<set /> 标签的 SqlNode 实现类。
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
 * <p>
 * 为什么需要继承TrimSqlNode？
 * Set 条件以 SET 字段作为前缀 prefix 字段，suffixToOverride 集合中的项为 “,” 。
 * 也就是说，<set> 节点解析之后的SQL语句片段，如果已 “,” 结尾，
 * 那么将结尾处的 “,” 删除后，再将 SET 关键字添加到SQL片段开始位置，从而到到该 <set> 节点最终生成的SQL片段。
 * <p>
 * {@link TrimSqlNode##FilteredDynamicContext}
 *
 * @author Clinton Begin
 */
public class SetSqlNode extends TrimSqlNode {

  private static final List<String> COMMA = Collections.singletonList(",");

  public SetSqlNode(Configuration configuration, SqlNode contents) {
    super(configuration, contents, "SET", COMMA, null, COMMA);
  }

}
