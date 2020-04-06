/**
 *    Copyright 2009-2020 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
/**
 * Contains the statement executors.
 * SQL执行模块
 * Executor 主要负责维护一级缓存和二级缓存，并提供事务管理的相关操作，它会将数据库相关操作委托给 StatementHandler完成。
 * StatementHandler 首先通过 ParameterHandler 完成 SQL 语句的实参绑定，
 * 然后通过 java.sql.Statement 对象执行 SQL 语句并得到结果集，
 * 最后通过 ResultSetHandler 完成结果集的映射，得到结果对象并返回。
 */
package org.apache.ibatis.executor;
