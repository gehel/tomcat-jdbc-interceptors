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

/**
 * Creates {@link Proxy}s to {@link Statement}s objects.
 *
 * @author gehel
 */
public class ProxyFactory {

    /** Used to report Metrics. */
    private final Metrics metrics;

    /**
     * Creates a proxy factory ready to report metrics to a Statsd server.
     *
     * @param metrics
     *            used to report Metrics
     */
    public ProxyFactory(final Metrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Creates a proxy around a {@link Statement}, reporting metrics when
     * called.
     *
     * @param statement the {@link Statement} to proxy
     * @return a proxied {@link Statement}
     */
    public final Statement statementProxy(final Statement statement) {
        return createProxy(Statement.class, new StatementInvocationHandler(
                statement, metrics));
    }

    /**
     * Creates a proxy around a {@link PreparedStatement}, reporting metrics
     * when called.
     *
     * @param preparedStatement
     *            the {@link PreparedStatement} to proxy
     * @return a proxied {@link PreparedStatement}
     */
    public final PreparedStatement preparedStatementProxy(
            final PreparedStatement preparedStatement) {
        return createProxy(PreparedStatement.class,
                new StatementInvocationHandler(preparedStatement, metrics));
    }

    /**
     * Creates a proxy around a {@link CallableStatement}, reporting metrics
     * when called.
     *
     * @param callableStatement
     *            the {@link CallableStatement} to proxy
     * @return a proxied {@link CallableStatement}
     */
    public final CallableStatement callableStatementProxy(
            final CallableStatement callableStatement) {
        return createProxy(CallableStatement.class,
                new StatementInvocationHandler(callableStatement, metrics));
    }

    /**
     * Internal creation of the proxy.
     * @param clazz Class to proxy
     * @param handler proxy implementation
     * @param <T> class being proxied
     * @return the proxy
     */
    private <T> T createProxy(final Class<T> clazz,
            final InvocationHandler handler) {
        return clazz.cast(Proxy.newProxyInstance(handler.getClass()
                .getClassLoader(), new Class[] {clazz}, handler));
    }

}
