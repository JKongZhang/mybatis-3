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

import java.util.regex.Pattern;

import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.parsing.TokenHandler;
import org.apache.ibatis.scripting.ScriptingException;
import org.apache.ibatis.type.SimpleTypeRegistry;

/**
 * 实现 SqlNode 接口，文本的 SqlNode 实现类。
 * 相比 StaticTextSqlNode 的实现来说，TextSqlNode 不确定是否为静态文本，
 * 所以提供 #isDynamic() 方法，进行判断是否为动态文本。
 *
 * @author Clinton Begin
 */
public class TextSqlNode implements SqlNode {
  /**
   * 文本
   */
  private final String text;
  /**
   * 目前该属性只在单元测试中使用，暂时无视
   */
  private final Pattern injectionFilter;

  public TextSqlNode(String text) {
    this(text, null);
  }

  public TextSqlNode(String text, Pattern injectionFilter) {
    this.text = text;
    this.injectionFilter = injectionFilter;
  }

  /**
   * 判断是否为动态文本。只要存在 ${xxx} 对，就认为是动态文本。
   *
   * @return
   */
  public boolean isDynamic() {
    // 1. 创建 DynamicCheckerTokenParser 对象
    DynamicCheckerTokenParser checker = new DynamicCheckerTokenParser();
    // 2. 创建 GenericTokenParser 对象
    GenericTokenParser parser = createParser(checker);
    // 3. 执行解析
    parser.parse(text);
    // 4. 判断是否为动态文本
    return checker.isDynamic();
  }

  @Override
  public boolean apply(DynamicContext context) {
    // 1. 创建 BindingTokenParser & GenericTokenParser 对象
    GenericTokenParser parser = createParser(new BindingTokenParser(context, injectionFilter));
    // 2. 执行解析，将解析的结果，添加到 context 中
    // 调用 GenericTokenParser#parse(String text) 方法，执行解析。
    // 当解析到 ${xxx} 时，会调用 BindingTokenParser 的 #handleToken(String content) 方法，执行相应的逻辑。
    context.appendSql(parser.parse(text));
    return true;
  }

  private GenericTokenParser createParser(TokenHandler handler) {
    return new GenericTokenParser("${", "}", handler);
  }

  private static class BindingTokenParser implements TokenHandler {

    private DynamicContext context;
    private Pattern injectionFilter;

    public BindingTokenParser(DynamicContext context, Pattern injectionFilter) {
      this.context = context;
      this.injectionFilter = injectionFilter;
    }

    /**
     * <p>
     * 对于该方法，如下的示例：
     * SELECT * FROM subject WHERE id = ${id}
     * id = ${id} 的 ${id} 部分，将被替换成对应的具体编号。
     * 例如说，id 为 1 ，则会变成 SELECT * FROM subject WHERE id = 1 。
     * <p>
     * 而对于如下的示例：
     * SELECT * FROM subject WHERE id = #{id}
     * id = #{id} 的 #{id} 部分，则不会进行替换。
     *
     * @param content 待处理的SQL
     * @return
     */
    @Override
    public String handleToken(String content) {
      // 初始化 value 属性到 context 中
      Object parameter = context.getBindings().get("_parameter");
      if (parameter == null) {
        context.getBindings().put("value", null);
      } else if (SimpleTypeRegistry.isSimpleType(parameter.getClass())) {
        context.getBindings().put("value", parameter);
      }

      // 使用 OGNL 表达式，获得对应的值
      Object value = OgnlCache.getValue(content, context.getBindings());
      String srtValue = value == null ? "" : String.valueOf(value); // issue #274 return "" instead of "null"
      checkInjection(srtValue);
      // 返回该值
      return srtValue;
    }

    private void checkInjection(String value) {
      if (injectionFilter != null && !injectionFilter.matcher(value).matches()) {
        throw new ScriptingException("Invalid input. Please conform to regex" + injectionFilter.pattern());
      }
    }
  }

  /**
   * 动态检查占位符的解析器
   */
  private static class DynamicCheckerTokenParser implements TokenHandler {
    /**
     * 是否为动态文本
     */
    private boolean isDynamic;

    public DynamicCheckerTokenParser() {
      // Prevent Synthetic Access
    }

    public boolean isDynamic() {
      return isDynamic;
    }

    /**
     * 当 GenericTokenParser 发现 token 时，
     * 会调用 #handleToken(String content) 方法，标记 isDynamic 为 true ，即标记为动态文本。
     */
    @Override
    public String handleToken(String content) {
      this.isDynamic = true;
      return null;
    }
  }

}
