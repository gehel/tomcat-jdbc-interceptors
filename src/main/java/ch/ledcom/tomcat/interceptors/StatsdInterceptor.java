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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.JdbcInterceptor;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.apache.tomcat.jdbc.pool.PoolProperties.InterceptorProperty;
import org.apache.tomcat.jdbc.pool.PooledConnection;

/**
 * {@link JdbcInterceptor} reporting timing of certain method calls to a Statsd
 * server.
 *
 * @author gehel
 */
public class StatsdInterceptor extends JdbcInterceptor {

    /** Only report metrics for those methods. */
    private static final Set<String> METHODS_TO_REPORT = new HashSet<String>(
            Arrays.asList("commit", "createStatement", "getMetadata",
                    "prepareCall", "prepareStatement", "rollback"));
    /** {@link Metrics} used for reporting. */
    private Metrics metrics;

    /** Factory to create {@link Statement} proxy. */
    private ProxyFactory proxyFactory;

    /**
     * This interceptor has no state to be reset, so this method does nothing.
     *
     * @see JdbcInterceptor#reset(ConnectionPool, PooledConnection)
     * @param parent
     *            the connection pool owning the connection
     * @param conn
     *            the pooled connection
     */
    @Override
    public void reset(final ConnectionPool parent,
            final PooledConnection conn) {
        // do nothing on reset, this interceptor has no state except
        // configuration
    }

    /**
     * Configure the interceptor.
     *
     * The following options are all required :
     * <ul>
     * <li>hostname: the hostname of the Statsd server</li>
     * <li>port: the port of the Statsd server</li>
     * <li>sampleRate: the fraction of connections that will be measured</li>
     * <li>prefix: this prefix will be added to the keys published to
     * Statsd</li>
     * </ul>
     *
     * @param properties configuration for the interceptor
     */
    @Override
    public final void setProperties(
            final Map<String, PoolProperties.InterceptorProperty> properties) {
        super.setProperties(properties);
        InterceptorProperty hostnameProp = properties.get("hostname");
        if (hostnameProp == null) {
            throw new IllegalArgumentException(
                    "property \"hostname\" has not been set");
        }
        InterceptorProperty portProp = properties.get("port");
        if (portProp == null) {
            throw new IllegalArgumentException(
                    "property \"port\" has not been set");
        }
        InterceptorProperty sampleRateProp = properties.get("sampleRate");
        if (sampleRateProp == null) {
            throw new IllegalArgumentException(
                    "property \"sampleRate\" has not been set");
        }
        InterceptorProperty prefixProp = properties.get("prefix");
        if (prefixProp == null) {
            throw new IllegalArgumentException(
                    "property \"prefix\" has not been set");
        }

        double sampleRate = sampleRateProp.getValueAsDouble(1.0);
        metrics = new Metrics(hostnameProp.getValue(),
                portProp.getValueAsInt(0), prefixProp.getValue(), sampleRate);
        proxyFactory = new ProxyFactory(metrics);
    }

    /**
     * Gets invoked each time an operation on {@link java.sql.Connection} is
     * invoked.
     *
     * The timing and count of each method calls to {@link java.sql.Connection}
     * are sent to Statsd.
     *
     * {@inheritDoc}
     */
    @Override
    public final Object invoke(final Object proxy, final Method method,
            final Object[] args) throws Throwable {
        String methodName = method.getName();
        boolean sample = metrics.sample();
        boolean shouldReport = shouldReport(methodName);
        long start = 0;
        if (sample && shouldReport) {
            start = System.nanoTime();
        }
        try {
            Object o = super.invoke(proxy, method, args);
            if (!sample) {
                // if this call is not sampled, no need to proxy the statements
            } else if ("createStatement".equals(methodName)) {
                o = proxyFactory.statementProxy((Statement) o);
            } else if ("prepareStatement".equals(method.getName())) {
                o = proxyFactory.preparedStatementProxy((PreparedStatement) o);
            } else if ("prepareCall".equals(method.getName())) {
                o = proxyFactory.callableStatementProxy((CallableStatement) o);
            }
            return o;
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            if (sample && shouldReport) {
                metrics.timing(".connection." + methodName + ".timing",
                        System.nanoTime() - start);
            }
        }
    }

    /**
     * Decide if we should report metrics for a specific method or not.
     *
     * @param methodName
     *            name of the method
     * @return <code>true</code> if we should report metrics
     */
    private boolean shouldReport(final String methodName) {
        return METHODS_TO_REPORT.contains(methodName);
    }

}
