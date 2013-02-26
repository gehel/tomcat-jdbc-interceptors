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

public class StatementInvocationHandler implements InvocationHandler {

    private final Statement statement;
    private final Metrics metrics;

    public StatementInvocationHandler(Statement statement, Metrics metrics) {
        this.statement = statement;
        this.metrics = metrics;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {

        long start = System.nanoTime();
        String methodName = method.getName();
        try {
            return method.invoke(statement, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            metrics.increment(".statement." + methodName + ".count");
            metrics.timing(".statement." + methodName + ".timing",
                    System.nanoTime() - start);
        }
    }
}