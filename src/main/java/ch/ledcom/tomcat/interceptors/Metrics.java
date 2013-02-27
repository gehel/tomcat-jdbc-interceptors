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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Locale;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reports metrics to a Statsd server.
 *
 * @author gehel
 */
public class Metrics {
    /** Standard logger. */
    private static final Logger LOG = Logger.getLogger(Metrics.class.getName());
    /** Random number generator used to decide if we sample a specific call. */
    static final Random RNG = new Random();

    /** Size of send buffer. */
    private static final int BUFFER_SIZE = 1500;

    /** Buffer used for communication with Statsd. */
    private final ByteBuffer sendBuffer;

    /** Address of the Statsd server. */
    private final InetSocketAddress address;
    /** UDP channel to the Statsd server. */
    private final DatagramChannel channel;
    /** Prepended to the key being reported. */
    private final String prefix;
    /** Ratio of metrics being actually reported. */
    private final double sampleRate;

    /**
     * Construct a reporter for a specific Statsd server.
     *
     * @param hostname
     *            hostname of the Statsd server
     * @param port
     *            port of the Statsd server
     * @param prefix
     *            prepended to the key being reported
     * @param sampleRate
     *            ratio of metrics being actually reported
     */
    public Metrics(final String hostname, final int port, final String prefix,
            final double sampleRate) {
        address = new InetSocketAddress(hostname, port);
        try {
            channel = DatagramChannel.open();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        this.prefix = prefix;
        sendBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.sampleRate = sampleRate;
    }

    /**
     * Report a timing metric to Statsd server.
     *
     * @param key
     *            key under which to report the metric
     * @param value
     *            time to report (in nanoseconds, even if Statsd expects
     *            milliseconds)
     */
    public final void timing(final String key, final long value) {
        doSend(String.format(Locale.ENGLISH, "%s:%d|ms|@%f", prefix + key,
                value, sampleRate));
    }

    /**
     * Internal sending of metrics.
     *
     * @param stat
     *            actual {@link String} to send
     */
    private synchronized void doSend(final String stat) {
        // TODO: This code is take directly from the Statsd example. In this
        // case, it could be greatly simplified and synchronization should be
        // reduced.
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
        } catch (IOException e) {
            LOG.log(Level.WARNING, String.format(
                    "Could not send stat %s to host %s:%d",
                    sendBuffer.toString(), address.getHostName(),
                    address.getPort()), e);
        }
    }

    /**
     * Flush send buffer.
     *
     * @return if flush actually happens
     */
    private synchronized boolean flush() {
        // TODO: This code is take directly from the Statsd example. In this
        // case, it could be greatly simplified and synchronization should be
        // reduced.
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
                LOG.log(Level.WARNING, String.format(
                        "Could not send entirely stat %s to host "
                                + "%s:%d. Only sent %d bytes out of %d bytes",
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

    /**
     * Check if we should sample a specific method call.
     *
     * As we don't want to impact performances too much, we only sample a given
     * ratio of calls. Based on the <code>sampleRate</code> property, we decide
     * if we wnat to sample this call.
     *
     * @return <code>true</code> if we should sample this call
     */
    public final boolean sample() {
        return RNG.nextDouble() <= sampleRate;
    }

}
