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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.JdbcInterceptor;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.apache.tomcat.jdbc.pool.PoolProperties.InterceptorProperty;
import org.apache.tomcat.jdbc.pool.PooledConnection;

public class StatsdInterceptor extends JdbcInterceptor {

    private static final int BUFFER_SIZE = 1500;

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    private static final Random RNG = new Random();

    private static final Logger LOG = Logger.getLogger(StatsdInterceptor.class
            .getName());

    private InetSocketAddress address;
    private DatagramChannel channel;
    private double sampleRate;
    private String prefix;

    /**
     * This interceptor has no state to be reset, so this method does nothing.
     * 
     * @see JdbcInterceptor#reset(ConnectionPool, PooledConnection)
     * @param parent
     *            the connection pool owning the connection
     * @param con
     *            the pooled connection
     */
    @Override
    public void reset(final ConnectionPool parent, final PooledConnection conn) {
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
     * <li>prefix: this prefix will be added to the keys published to Statsd</li>
     * </ul>
     * 
     * @param properties
     */
    @Override
    public synchronized void setProperties(
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

        String hostname = hostnameProp.getValue();
        int port = portProp.getValueAsInt(0);
        sampleRate = sampleRateProp.getValueAsDouble(1.0);
        prefix = prefixProp.getValue();
        address = new InetSocketAddress(hostname, port);
        try {
            channel = DatagramChannel.open();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
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
        long start = System.nanoTime();
        Object result = super.invoke(proxy, method, args);

        increment(methodName + ".count");
        timing(methodName + ".timing", System.nanoTime() - start);
        return result;
    }

    private boolean increment(final String key) {
        return send(String.format(Locale.ENGLISH, "%s:1|c", prefix + key));
    }

    private boolean timing(final String key, final long value) {
        return send(String.format(Locale.ENGLISH, "%s:%d|ms", prefix + key, value));
    }

    private boolean send(String stat) {

        boolean retval = false; // didn't send anything
        if ((sampleRate < 1.0) && (RNG.nextDouble() <= sampleRate)) {
            stat = String.format(Locale.ENGLISH, "%s|@%f", stat, sampleRate);
        }
        if (doSend(stat)) {
            retval = true;
        }

        return retval;
    }

    private synchronized boolean doSend(final String stat) {
        try {
            final byte[] data = stat.getBytes("utf-8");

            // If we're going to go past the threshold of the buffer then flush.
            // the +1 is for the potential '\n' in multi_metrics below
            if (sendBuffer.remaining() < (data.length + 1)) {
                flush();
            }

            if (sendBuffer.position() > 0) { // multiple metrics are separated
                                             // by '\n'
                sendBuffer.put((byte) '\n');
            }

            sendBuffer.put(data); // append the data

            flush();

            return true;

        } catch (IOException e) {
            LOG.log(Level.WARNING, String.format(
                    "Could not send stat %s to host %s:%d",
                    sendBuffer.toString(), address.getHostName(),
                    address.getPort()), e);
            return false;
        }
    }

    private synchronized boolean flush() {
        try {
            final int sizeOfBuffer = sendBuffer.position();

            if (sizeOfBuffer <= 0) {
                return false;
            } // empty buffer

            // send and reset the buffer
            sendBuffer.flip();
            final int nbSentBytes = channel.send(sendBuffer, address);
            sendBuffer.limit(sendBuffer.capacity());
            sendBuffer.rewind();

            if (sizeOfBuffer == nbSentBytes) {
                return true;
            } else {
                LOG.log(Level.WARNING,
                        String.format(
                                "Could not send entirely stat %s to host %s:%d. Only sent %d bytes out of %d bytes",
                                sendBuffer.toString(), address.getHostName(),
                                address.getPort(), nbSentBytes, sizeOfBuffer));
                return false;
            }

        } catch (IOException e) {
            LOG.log(Level.WARNING, String.format(
                    "Could not send stat %s to host %s:%d",
                    sendBuffer.toString(), address.getHostName(),
                    address.getPort()), e);
            return false;
        }
    }
}
