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
import org.apache.tomcat.jdbc.pool.PooledConnection;

public class StatsdInterceptor extends JdbcInterceptor {

    private static final int BUFFER_SIZE = 1500;

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    private static final Random RNG = new Random();

    private static final Logger log = Logger.getLogger(StatsdInterceptor.class
            .getName());

    private InetSocketAddress address;
    private DatagramChannel channel;
    private double sampleRate;

    @Override
    public void reset(ConnectionPool arg0, PooledConnection arg1) {
        // do nothing on reset, this interceptor has no state except
        // configuration
    }

    @Override
    public void setProperties(
            Map<String, PoolProperties.InterceptorProperty> properties) {
        String hostname = null;
        int port = 0;
        address = new InetSocketAddress(hostname, port);
        try {
            channel = DatagramChannel.open();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        String methodName = method.getName();
        long start = System.nanoTime();
        Object result = super.invoke(proxy, method, args);

        increment(methodName);
        timing(methodName, System.nanoTime() - start);
        return result;
    }

    private boolean increment(String key) {
        String stat = String.format(Locale.ENGLISH, "%s:1|c", key);
        return send(sampleRate, stat);
    }

    private boolean timing(String key, long value) {
        return send(sampleRate,
                String.format(Locale.ENGLISH, "%s:%d|ms", key, value));
    }

    private boolean send(double sampleRate, String... stats) {

        boolean retval = false; // didn't send anything
        if (sampleRate < 1.0) {
            for (String stat : stats) {
                if (RNG.nextDouble() <= sampleRate) {
                    stat = String.format(Locale.ENGLISH, "%s|@%f", stat,
                            sampleRate);
                    if (doSend(stat)) {
                        retval = true;
                    }
                }
            }
        } else {
            for (String stat : stats) {
                if (doSend(stat)) {
                    retval = true;
                }
            }
        }

        return retval;
    }

    private synchronized boolean doSend(String stat) {
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
            log.log(Level.WARNING, String.format(
                    "Could not send stat %s to host %s:%d",
                    sendBuffer.toString(), address.getHostName(),
                    address.getPort()), e);
            return false;
        }
    }

    public synchronized boolean flush() {
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
                log.log(Level.WARNING,
                        String.format(
                                "Could not send entirely stat %s to host %s:%d. Only sent %d bytes out of %d bytes",
                                sendBuffer.toString(), address.getHostName(),
                                address.getPort(), nbSentBytes, sizeOfBuffer));
                return false;
            }

        } catch (IOException e) {
            log.log(Level.WARNING, String.format(
                    "Could not send stat %s to host %s:%d",
                    sendBuffer.toString(), address.getHostName(),
                    address.getPort()), e);
            return false;
        }
    }
}
