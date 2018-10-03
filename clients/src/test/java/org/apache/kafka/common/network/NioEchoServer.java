/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;

/**
 * Non-blocking EchoServer implementation that uses ChannelBuilder to create channels
 * with the configured security protocol.
 *
 */
public class NioEchoServer extends Thread {

    private static final double EPS = 0.0001;

    private final int port;
    private final ServerSocketChannel serverSocketChannel;
    private final List<SocketChannel> newChannels;
    private final List<SocketChannel> socketChannels;
    private final AcceptorThread acceptorThread;
    private final Selector selector;
    private volatile WritableByteChannel outputChannel;
    private final CredentialCache credentialCache;
    private final Metrics metrics;
    private volatile int numSent = 0;
    private volatile boolean closeKafkaChannels;
    private final DelegationTokenCache tokenCache;

    public NioEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol, AbstractConfig config,
                         String serverHost, ChannelBuilder channelBuilder, CredentialCache credentialCache, Time time) throws Exception {
        this(listenerName, securityProtocol, config, serverHost, channelBuilder, credentialCache, 100, time);
    }

    public NioEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol, AbstractConfig config,
                         String serverHost, ChannelBuilder channelBuilder, CredentialCache credentialCache,
                         int failedAuthenticationDelayMs, Time time) throws Exception {
        this(listenerName, securityProtocol, config, serverHost, channelBuilder, credentialCache, 100, time,
                new DelegationTokenCache(ScramMechanism.mechanismNames()));
    }

    public NioEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol, AbstractConfig config,
            String serverHost, ChannelBuilder channelBuilder, CredentialCache credentialCache,
            int failedAuthenticationDelayMs, Time time, DelegationTokenCache tokenCache) throws Exception {
        super("echoserver");
        setDaemon(true);
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(serverHost, 0));
        this.port = serverSocketChannel.socket().getLocalPort();
        this.socketChannels = Collections.synchronizedList(new ArrayList<SocketChannel>());
        this.newChannels = Collections.synchronizedList(new ArrayList<SocketChannel>());
        this.credentialCache = credentialCache;
        this.tokenCache = tokenCache;
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            for (String mechanism : ScramMechanism.mechanismNames()) {
                if (credentialCache.cache(mechanism, ScramCredential.class) == null)
                    credentialCache.createCache(mechanism, ScramCredential.class);
            }
        }
        if (channelBuilder == null)
            channelBuilder = ChannelBuilders.serverChannelBuilder(listenerName, false, securityProtocol, config, credentialCache, tokenCache, time);
        this.metrics = new Metrics();
        this.selector = new Selector(10000, failedAuthenticationDelayMs, metrics, time, "MetricGroup", channelBuilder, new LogContext());
        acceptorThread = new AcceptorThread();
    }

    public int port() {
        return port;
    }

    public CredentialCache credentialCache() {
        return credentialCache;
    }

    public DelegationTokenCache tokenCache() {
        return tokenCache;
    }

    public double metricValue(String name) {
        for (Map.Entry<MetricName, KafkaMetric> entry : metrics.metrics().entrySet()) {
            if (entry.getKey().name().equals(name))
                return (double) entry.getValue().metricValue();
        }
        throw new IllegalStateException("Metric not found, " + name + ", found=" + metrics.metrics().keySet());
    }

    public void verifyAuthenticationMetrics(int successfulAuthentications, final int failedAuthentications)
            throws InterruptedException {
        waitForMetric("successful-authentication", successfulAuthentications, true, true, false, false,
                metricForensics());
        waitForMetric("failed-authentication", failedAuthentications, true, true, false, false, metricForensics());
    }

    public void verifyReauthenticationMetrics(int successfulReauthentications, final int failedReauthentications)
            throws InterruptedException {
        waitForMetric("successful-reauthentication", successfulReauthentications, true, true, false, false,
                metricForensics());
        waitForMetric("failed-reauthentication", failedReauthentications, true, true, false, false, metricForensics());
    }

    public void verifyAuthenticationMetrics(int successfulAuthentications, final int failedAuthentications,
            int successfulReauthentications, final int failedReauthentications, boolean includeReauthenticationLatency,
            int successfulAuthenticationNoReauths)
            throws InterruptedException {
        verifyAuthenticationMetrics(successfulAuthentications, failedAuthentications);
        if (includeReauthenticationLatency)
            verifyReauthenticationMetricsIncludingLatency(successfulReauthentications, failedReauthentications);
        else
            verifyReauthenticationMetrics(successfulReauthentications, failedReauthentications);
        verifyAuthenticationNoReauthMetric(successfulAuthenticationNoReauths);
    }

    public void verifyReauthenticationMetricsIncludingLatency(int successfulReauthentications,
            final int failedReauthentications) throws InterruptedException {
        verifyReauthenticationMetrics(successfulReauthentications, failedReauthentications);
        // non-zero re-authentications implies some latency recorded
        waitForMetric("reauthentication-latency", Math.signum(successfulReauthentications), false, false, true, true,
                metricForensics());
    }

    public void verifyAuthenticationNoReauthMetric(int successfulAuthenticationNoReauths) throws InterruptedException {
        waitForMetric("successful-authentication-no-reauth", successfulAuthenticationNoReauths, true, false);
    }

    public void waitForMetric(String name, final double expectedValue) throws InterruptedException {
        waitForMetric(name, expectedValue, true, true);
    }

    public void waitForMetric(String name, final double expectedValue, boolean total, boolean rate)
            throws InterruptedException {
        waitForMetric(name, expectedValue, total, rate, false, false, null);
    }

    public void waitForMetric(String name, final double expectedValue, boolean total, boolean rate, boolean avg, boolean max,
            BiFunction<String, String, String> forensics) throws InterruptedException {
        waitForTotalAndRateMetrics(name, expectedValue, total, rate, forensics);
        waitForAvgAndMaxMetrics(name, expectedValue, avg, max, forensics);
    }

    public void waitForTotalAndRateMetrics(String name, final double expectedValue, boolean total, boolean rate,
            BiFunction<String, String, String> forensics) throws InterruptedException {
        final String totalName = name + "-total";
        final String rateName = name + "-rate";
        try {
            if (expectedValue == 0.0) {
                if (total)
                    assertEquals(expectedValue, metricValue(totalName), EPS);
                if (rate)
                    assertEquals(expectedValue, metricValue(rateName), EPS);
            } else {
                if (total)
                    TestUtils.waitForCondition(() -> Math.abs(metricValue(totalName) - expectedValue) <= EPS,
                            "Metric not updated " + totalName);
                if (rate)
                    TestUtils.waitForCondition(() -> metricValue(rateName) > 0.0, "Metric not updated " + rateName);
            }
        } catch (AssertionError e) {
            if (forensics == null)
                throw e;
            String forensicText = e.getMessage() + ": "
                    + forensics.apply(total ? totalName : null, rate ? rateName : null);
            throw new AssertionError(forensicText, e);
        }
    }

    public void waitForAvgAndMaxMetrics(String name, final double expectedValue, boolean avg, boolean max,
            BiFunction<String, String, String> forensics) throws InterruptedException {
        final String avgName = name + "-avg";
        final String maxName = name + "-max";
        try {
            if (expectedValue == 0.0) {
                if (avg)
                    assertEquals(expectedValue, metricValue(avgName), EPS);
                if (max)
                    assertEquals(Double.NEGATIVE_INFINITY, metricValue(maxName), EPS);
            } else {
                if (avg)
                    TestUtils.waitForCondition(() -> metricValue(avgName) > 0.0, "Metric not updated " + avgName);
                if (max)
                    TestUtils.waitForCondition(() -> metricValue(maxName) > 0.0, "Metric not updated " + maxName);
            }
        } catch (AssertionError e) {
            if (forensics == null)
                throw e;
            String forensicText = e.getMessage() + ": "
                    + forensics.apply(avg ? avgName : null, max ? maxName : null);
            throw new AssertionError(forensicText, e);
        }
    }

    @Override
    public void run() {
        try {
            acceptorThread.start();
            while (serverSocketChannel.isOpen()) {
                selector.poll(1000);
                synchronized (newChannels) {
                    for (SocketChannel socketChannel : newChannels) {
                        String id = id(socketChannel);
                        selector.register(id, socketChannel);
                        socketChannels.add(socketChannel);
                    }
                    newChannels.clear();
                }
                if (closeKafkaChannels) {
                    for (KafkaChannel channel : selector.channels())
                        selector.close(channel.id());
                }

                List<NetworkReceive> completedReceives = selector.completedReceives();
                for (NetworkReceive rcv : completedReceives) {
                    KafkaChannel channel = channel(rcv.source());
                    if (!TestUtils.maybeBeginServerReauthentication(channel, rcv)) {
                        String channelId = channel.id();
                        selector.mute(channelId);
                        NetworkSend send = new NetworkSend(rcv.source(), rcv.payload());
                        if (outputChannel == null)
                            selector.send(send);
                        else {
                            for (ByteBuffer buffer : send.buffers)
                                outputChannel.write(buffer);
                            selector.unmute(channelId);
                        }
                    }
                }
                for (Send send : selector.completedSends()) {
                    selector.unmute(send.destination());
                    numSent += 1;
                }
            }
        } catch (IOException e) {
            // ignore
        }
    }

    public int numSent() {
        return numSent;
    }

    private String id(SocketChannel channel) {
        return channel.socket().getLocalAddress().getHostAddress() + ":" + channel.socket().getLocalPort() + "-" +
                channel.socket().getInetAddress().getHostAddress() + ":" + channel.socket().getPort();
    }

    private KafkaChannel channel(String id) {
        KafkaChannel channel = selector.channel(id);
        return channel == null ? selector.closingChannel(id) : channel;
    }

    private BiFunction<String, String, String> metricForensics() {
        return (metric1, metric2) -> {
            return String.format("Failed: %s=%s, %s=%s", metric1, metric1 == null ? "N/A" : metricValue(metric1),
                    metric2, metric2 == null ? "N/A" : metricValue(metric2));
        };
    }

    /**
     * Sets the output channel to which messages received on this server are echoed.
     * This is useful in tests where the clients sending the messages don't receive
     * the responses (eg. testing graceful close).
     */
    public void outputChannel(WritableByteChannel channel) {
        this.outputChannel = channel;
    }

    public Selector selector() {
        return selector;
    }

    public void closeKafkaChannels() throws IOException {
        closeKafkaChannels = true;
        selector.wakeup();
        try {
            TestUtils.waitForCondition(() -> selector.channels().isEmpty(), "Channels not closed");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            closeKafkaChannels = false;
        }
    }

    public void closeSocketChannels() throws IOException {
        for (SocketChannel channel : socketChannels) {
            channel.close();
        }
        socketChannels.clear();
    }

    public void close() throws IOException, InterruptedException {
        this.serverSocketChannel.close();
        closeSocketChannels();
        acceptorThread.interrupt();
        acceptorThread.join();
        interrupt();
        join();
    }

    private class AcceptorThread extends Thread {
        public AcceptorThread() throws IOException {
            setName("acceptor");
        }
        public void run() {
            try {
                java.nio.channels.Selector acceptSelector = java.nio.channels.Selector.open();
                serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
                while (serverSocketChannel.isOpen()) {
                    if (acceptSelector.select(1000) > 0) {
                        Iterator<SelectionKey> it = acceptSelector.selectedKeys().iterator();
                        while (it.hasNext()) {
                            SelectionKey key = it.next();
                            if (key.isAcceptable()) {
                                SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
                                socketChannel.configureBlocking(false);
                                newChannels.add(socketChannel);
                                selector.wakeup();
                            }
                            it.remove();
                        }
                    }
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
