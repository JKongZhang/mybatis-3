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

/**
 * 主要针对动态SQL中的 <bind> 节点。
 * 该节点可以从OGNL表达式中创建一个变量，并将其记录到上下文中。
 * 在 VarDeclSqlNode 中使用 name 字段记录<bind> 节点的 name 属性值。
 * expression 字段记录 <bind> 字段的value 属性值。
 *
 * @author Frank D. Martinez [mnesarco]
 */
public class VarDeclSqlNode implements SqlNode {

  /**
   * 名字
   */
  private final String name;
  /**
   * 表达式
   */
  private final String expression;

  /**
   * <bind name="pattern" value="'%' + _parameter.getTitle() + '%'" />
   *
   * @param name name 属性值
   * @param exp  value 属性值
   */
  public VarDeclSqlNode(String name, String exp) {
    this.name = name;
    expression = exp;
  }

  @Override
  public boolean apply(DynamicContext context) {
    final Object value = OgnlCache.getValue(expression, context.getBindings());
    context.bind(name, value);
    return true;
  }

}
