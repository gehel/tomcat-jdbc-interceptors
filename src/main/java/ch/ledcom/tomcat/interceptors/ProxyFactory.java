/**
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */
package ch.ledcom.tomcat.interceptors;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class ProxyFactory {

    private final Metrics metrics;

    public ProxyFactory(Metrics metrics) {
        this.metrics = metrics;
    }

    public Statement statementProxy(Statement statement) {
        return createProxy(Statement.class, new StatementInvocationHandler(
                statement, metrics));
    }

    public PreparedStatement preparedStatementProxy(
            PreparedStatement preparedStatement) {
        return createProxy(PreparedStatement.class,
                new StatementInvocationHandler(preparedStatement, metrics));
    }

    public CallableStatement callableStatementProxy(
            CallableStatement callableStatement) {
        return createProxy(CallableStatement.class,
                new StatementInvocationHandler(callableStatement, metrics));
    }

    private <T> T createProxy(Class<T> clazz, InvocationHandler handler) {
        return clazz.cast(Proxy.newProxyInstance(handler.getClass()
                .getClassLoader(), new Class[] { clazz }, handler));
    }

}
