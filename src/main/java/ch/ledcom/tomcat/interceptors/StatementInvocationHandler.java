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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link InvocationHandler} specialized in reporting metrics around
 * {@link Statement} operations.
 *
 * @author gehel
 */
public class StatementInvocationHandler implements InvocationHandler {
    /**
     * This {@link InvocationHandler} only reports metrics on this {@link Set}
     * of method names.
     */
    private static final Set<String> METHODS_TO_REPORT = new HashSet<String>(
            Arrays.asList("execute", "executeBatch", "executeQuery",
                    "executeUpdate"));

    /** {@link Statement} being proxied. */
    private final Statement statement;
    /** {@link Metrics} used for reporting. */
    private final Metrics metrics;

    /**
     * Creates the {@link InvocationHandler}.
     *
     * @param statement
     *            {@link Statement} being proxied
     * @param metrics
     *            {@link Metrics} used for reporting
     */
    public StatementInvocationHandler(final Statement statement,
            final Metrics metrics) {
        this.statement = statement;
        this.metrics = metrics;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Object invoke(final Object proxy, final Method method,
            final Object[] args) throws Throwable {
        String methodName = method.getName();
        long start = 0;
        if (shouldReport(methodName)) {
            start = System.nanoTime();
        }
        try {
            return method.invoke(statement, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            if (shouldReport(methodName)) {
                metrics.timing(".statement." + methodName + ".timing",
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
