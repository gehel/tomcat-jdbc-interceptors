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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StatsdInterceptorTest {

    private static final String STATSD_PREFIX = "jdbc.pool.";
    private static final int STATSD_PORT = 6545;
    private DataSource ds;
    private Thread statsdThread;
    private List<String> receivedMessages = new CopyOnWriteArrayList<String>();

    private final Object packetReceived = new Object();

    @Before
    public void createDataSource() {
        PoolProperties poolProperties = new PoolProperties();
        poolProperties.setDriverClassName(JDBCDriver.class.getName());
        poolProperties.setUrl("jdbc:hsqldb:mem:aname");
        poolProperties.setUsername("sa");
        poolProperties.setPassword("");
        poolProperties.setJdbcInterceptors(StatsdInterceptor.class.getName()
                + "(hostname=localhost,port=" + STATSD_PORT
                + ",sampleRate=1.0,prefix=" + STATSD_PREFIX + ")");
        ds = new DataSource();
        ds.setPoolProperties(poolProperties);
    }

    @Before
    public void startMockStatsd() {
        statsdThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    DatagramSocket serverSocket = new DatagramSocket(
                            STATSD_PORT);
                    serverSocket.setSoTimeout(500);
                    while (!Thread.currentThread().isInterrupted()) {
                        byte[] receiveData = new byte[1024];
                        DatagramPacket receivePacket = new DatagramPacket(
                                receiveData, receiveData.length);
                        serverSocket.receive(receivePacket);
                        String message = new String(receivePacket.getData(), 0,
                                receivePacket.getLength());
                        System.out.println(message);
                        receivedMessages.add(message);
                        synchronized (packetReceived) {
                            packetReceived.notifyAll();
                        }
                    }
                } catch (SocketTimeoutException expected) {
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        statsdThread.start();
    }

    @Test
    public void interceptorSendsPacketsToStatsd() throws SQLException,
            InterruptedException {
        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("Create table toto (a Integer)");
        stmt.close();
        conn.close();

        // give time to the packet to be received
        waitForFirstPacket();
        assertEquals("jdbc.pool.createStatement.count:1|c",
                receivedMessages.get(0));
    }

    @After
    public void closeDataSource() {
        ds.close();
    }

    @After
    public void stopMockStatsd() throws InterruptedException {
        statsdThread.interrupt();
        statsdThread.join();
    }

    private void waitForFirstPacket() throws InterruptedException {
        while (receivedMessages.size() == 0) {
            synchronized (packetReceived) {
                packetReceived.wait();
            }
        }
    }
}
