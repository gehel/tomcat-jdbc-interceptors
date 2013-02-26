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
import java.util.logging.Level;
import java.util.logging.Logger;

public class Metrics {
    private static final Logger LOG = Logger.getLogger(Metrics.class.getName());

    private static final int BUFFER_SIZE = 1500;

    private final ByteBuffer sendBuffer;

    private final InetSocketAddress address;
    private final DatagramChannel channel;
    private final String prefix;
    private final double sampleRate;

    public Metrics(String hostname, int port, String prefix, double sampleRate) {
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

    public boolean increment(final String key) {
        return doSend(String.format(Locale.ENGLISH, "%s:1|c|@%f", prefix + key,
                sampleRate));
    }

    public boolean timing(final String key, final long value) {
        return doSend(String.format(Locale.ENGLISH, "%s:%d|ms|@%f", prefix
                + key, value, sampleRate));
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
