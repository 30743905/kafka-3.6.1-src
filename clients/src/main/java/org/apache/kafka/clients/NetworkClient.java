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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CorrelationIdMismatchException;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private enum State {
        ACTIVE,
        CLOSING,
        CLOSED
    }

    private final Logger log;

    /* the selector used to perform network i/o */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* default timeout for individual requests to await acknowledgement from servers */
    private final int defaultRequestTimeoutMs;

    /* time in ms to wait before retrying to create connection to a server */
    private final long reconnectBackoffMs;

    private final Time time;

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    private final boolean discoverBrokerVersions;

    private final ApiVersions apiVersions;

    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();

    private final List<ClientResponse> abortedSends = new LinkedList<>();

    private final Sensor throttleTimeSensor;

    private final AtomicReference<State> state;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(selector,
             metadata,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext);
    }

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             throttleTimeSensor,
             logContext,
             new DefaultHostResolver());
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(metadataUpdater,
             null,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext,
             new DefaultHostResolver());
    }

    public NetworkClient(MetadataUpdater metadataUpdater,
                         Metadata metadata,
                         Selectable selector,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext,
                         HostResolver hostResolver) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(
                reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, logContext, hostResolver);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
        this.state = new AtomicReference<>(State.ACTIVE);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        //判断要发送消息的主机，是否具备发送消息的条件
        if (isReady(node, now))
            return true;

        //判断是否可以尝试去建立网络
        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            //如果 connectionStates.canConnect(node.idString(), now) 返回true，接下来调用建立网络连接的方法initiateConnect(node, now)
            initiateConnect(node, now);

        return false;
    }

    // Visible for testing
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    @Override
    public void disconnect(String nodeId) {
        if (connectionStates.isDisconnected(nodeId)) {
            log.debug("Client requested disconnect from node {}, which is already disconnected", nodeId);
            return;
        }

        log.info("Client requested disconnect from node {}", nodeId);
        selector.close(nodeId);
        long now = time.milliseconds();
        cancelInFlightRequests(nodeId, now, abortedSends, false);
        connectionStates.disconnected(nodeId, now);
    }

    private void cancelInFlightRequests(String nodeId,
                                        long now,
                                        Collection<ClientResponse> responses,
                                        boolean timedOut) {
        Iterable<InFlightRequest> inFlightRequests = this.inFlightRequests.clearAll(nodeId);
        for (InFlightRequest request : inFlightRequests) {
            if (log.isDebugEnabled()) {
                log.debug("Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                        "(elapsed time since creation: {}ms, elapsed time since send: {}ms, request timeout: {}ms): {}",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.requestTimeoutMs, request.request);
            } else {
                log.info("Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                        "(elapsed time since creation: {}ms, elapsed time since send: {}ms, request timeout: {}ms)",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.requestTimeoutMs);
            }

            if (!request.isInternalRequest) {
                if (responses != null) {
                    ClientResponse clientResponse;

                    if (timedOut)
                        clientResponse = request.timedOut(now);
                    else
                        clientResponse = request.disconnected(now);

                    responses.add(clientResponse);
                }
            } else if (request.header.apiKey() == ApiKeys.METADATA) {
                metadataUpdater.handleFailedRequest(now, Optional.empty());
            }
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        log.info("Client requested connection close from node {}", nodeId);
        selector.close(nodeId);
        long now = time.milliseconds();
        cancelInFlightRequests(nodeId, now, null, false);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    // Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
    // This is for testing.
    public long throttleDelayMs(Node node, long now) {
        return connectionStates.throttleDelayMs(node.idString(), now);
    }

    /**
     * Return the poll delay in milliseconds based on both connection and throttle delay.
     * @param node the connection to check
     * @param now the current time in ms
     */
    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionStates.pollDelayMs(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        /**
         * 元数据是否过期，判断条件：
         *  1. needUpdate被置为true
         *  2. 上次更新时间距离当前时间已经超过了指定的元数据过期时间阈值metadataExpireMs（metadata.max.age.ms），默认是300秒
         *
         *  !metadataUpdater.isUpdateDue(now) 当前不能处于元数据加载的过程，而且下一次要更新元数据的间隔时间为0，现在没有加载元数据，但是马上就应该要加载元数据了，
         *  如果对上述条件判断是非的话，要不然是正在加载元数据，或者是还没到加载元数据的时候。为什么一定要有这个条件？假设此时必须要更新元数据了，就不能发送请求，必须要等待这个元数据被刷新了再次去发送请求
         *
         */
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now);
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     * @param now the current timestamp
     */
    private boolean canSendRequest(String node, long now) {
        /**
         * connectionStates.isReady(node):
         *  生产者：多个连接，缓存多个连接（跟我们的broker的节点数是一样的）
         *     判断缓存里面是否已经把这个连接给建立好了。
         * selector.isChannelReady(node)：
         *      java NIO：selector
         *      selector -> 绑定了多个KafkaChannel(java socketChannel)
         *      一个kafkaChannel就代表一个连接。
         *
         *  inFlightRequests.canSendMore(node)：
         *  每个往broker主机上面发送消息的连接，最多能容忍5个消息（max.in.flight.requests.per.connection参数设置，默认5），发送出去了但是还没有接受到响应。
         *  发送数据的顺序。
         *  1,2,3,4,5
         *  有可能到生产者的顺序如下：
         *  2,3,4，5，1
         *
         *  再多说一下inFlightRequests 的参数，如果单个分区保证顺序性，这个参数必须设置成1
         */
        return connectionStates.isReady(node, now) && selector.isChannelReady(node) &&
            inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    // package-private for testing
    void sendInternalMetadataRequest(MetadataRequest.Builder builder, String nodeConnectionId, long now) {
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        doSend(clientRequest, true, now);
    }

    /**
     * 此方法逻辑如下：
     * 1、检查NetWorkClient状态为激活。
     * 2、取得请求发送目的地的nodeId。
     * 3、如果是非内部请求，检查connectionState是否ready、Channel是否ready、是否达到发送中上限
     * 4、通过ClientRequest携带的AbstractRequest.Builder对象获取的version以及目的地node的api version，来取得最终的version
     * 5、通过builder.build(version)方法，来初始化request，这里实际生成的是ProduceRequest。
     * 6、最后调用doSend(clientRequest, isInternalRequest, now, builder.build(version));
     *
     * 我们继续看doSend(clientRequest, isInternalRequest, now, builder.build(version))方法。
     * @param clientRequest
     * @param isInternalRequest
     * @param now
     */
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        ensureActive();
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            if (!canSendRequest(nodeId, now))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException unsupportedVersionException) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException);
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, unsupportedVersionException, null, null);

            if (!isInternalRequest)
                abortedSends.add(clientResponse);
            else if (clientRequest.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
        }
    }

    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        String destination = clientRequest.destination();
        RequestHeader header = clientRequest.makeHeader(request.version());
        if (log.isDebugEnabled()) {
            log.debug("Sending {} request with header {} and timeout {} to node {}: {}",
                clientRequest.apiKey(), header, clientRequest.requestTimeoutMs(), destination, request);
        }
        Send send = request.toSend(header);
        InFlightRequest inFlightRequest = new InFlightRequest(
                clientRequest,
                header,
                isInternalRequest,
                request,
                send,
                now);
        // InFlightRequest(飞行队列)表示请求已经发送，但是还没有得到响应
        this.inFlightRequests.add(inFlightRequest);
        selector.send(new NetworkSend(clientRequest.destination(), send));
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        ensureActive();

        if (!abortedSends.isEmpty()) {// 如果存在abortedSends，直接处理，不经过selector的poll
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            List<ClientResponse> responses = new ArrayList<>();
            /**
             * 处理那么在发送过程出现 UnsupportedVersionException 异常的 request；
             */
            handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        /**
         * 判断是否要更新Metadata元数据，如果需要的话发送更新请求（会选择inflight请求数量最少的ready node发送请求）
         */
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            // 完成一次selector poll，最大阻塞时间为timeout，metadataTimeout，requestTimeoutMs的最小值
            this.selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        long updatedNow = this.time.milliseconds();
        // 初始化response list
        List<ClientResponse> responses = new ArrayList<>();
        /**
         * 处理那些已经完成的 request，发送完成的请求放入到completedSends集合中
         * 如果是那些不需要 response 的 request 的话，这里直接调用 request.completed()，标志着这个 request 发送处理完成
         */
        handleCompletedSends(responses, updatedNow);
        // 根据inFlightRequests的发送顺序生成已完成回复的response放入list
        /**
         * 处理那些从 Server 端接收的 Receive，metadata 更新就是在这里处理的（以及 ApiVersionsResponse）；
         */
        handleCompletedReceives(responses, updatedNow);
        /**
         * 处理连接失败那些连接,重新请求 metadata；
         */
        handleDisconnections(responses, updatedNow);
        /**
         * 处理新建立的那些连接（还不能发送请求,比如:还未认证）；
         */
        handleConnections();
        // 需要的话发送api version获取请求
        /**
         * 对那些新建立的连接，发送 apiVersionRequest（默认情况：第一次建立连接时，需要向 Broker 发送 ApiVersionRequest 请求）；
         */
        handleInitiateApiVersionRequests(updatedNow);
        /**
         * 处理 timeout 的连接，关闭该连接，并刷新 Metadata。
         */
        handleTimedOutConnections(responses, updatedNow);
        // 处理超时请求，生成response放入list
        handleTimedOutRequests(responses, updatedNow);
        //
        /**
         * 最终所有 response 都会在 completeResponses 中处理，而处理所调用的方法就是我们在构建 request 时传入的 callback.onComplete(response)。
         * 所以回到 Sender 类中的 handleProducerResponse 方法就可以看到 producer 收到 broker 回复后的处理逻辑了。
         */
        completeResponses(responses);

        return responses;
    }

    private void completeResponses(List<ClientResponse> responses) {
        /**
         * 循环ClientResponse列表，调用ClientResponse的onComplete方法。此方法中实际调用了ClientResponse相关联的callback对象的onComplte方法。
         * 这个callback对象是我们在生成ClientRequest之前声明的，在Sender的sendProduceRequest方法中：
         *      RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());
         * 可以看到最终调用了Sender#handleProduceResponse方法
         */
        for (ClientResponse response : responses) {
            try {
                // 调用response中的callback进行处理，producer端可以参考Sender类的handleProduceResponse方法
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    @Override
    public boolean hasReadyNodes(long now) {
        return connectionStates.hasReadyNodes(now);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    @Override
    public void initiateClose() {
        if (state.compareAndSet(State.ACTIVE, State.CLOSING)) {
            wakeup();
        }
    }

    @Override
    public boolean active() {
        return state.get() == State.ACTIVE;
    }

    private void ensureActive() {
        if (!active())
            throw new DisconnectException("NetworkClient is no longer active, state is " + state);
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        state.compareAndSet(State.ACTIVE, State.CLOSING);
        if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
            this.selector.close();
            this.metadataUpdater.close();
        } else {
            log.warn("Attempting to close NetworkClient that has already been closed.");
        }
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. If no connection exists, this method will prefer a node
     * with least recent connection attempts. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period, or an active
     * connection which is being throttled.
     *
     * @return The node with the fewest in-flight requests.
     */
    @Override
    public Node leastLoadedNode(long now) {
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        if (nodes.isEmpty())
            throw new IllegalStateException("There are no nodes in the Kafka cluster");
        int inflight = Integer.MAX_VALUE;

        Node foundConnecting = null;
        Node foundCanConnect = null;
        Node foundReady = null;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            if (canSendRequest(node.idString(), now)) {
                int currInflight = this.inFlightRequests.count(node.idString());
                if (currInflight == 0) {
                    // if we find an established connection with no in-flight requests we can stop right away
                    log.trace("Found least loaded node {} connected with no in-flight requests", node);
                    return node;
                } else if (currInflight < inflight) {
                    // otherwise if this is the best we have found so far, record that
                    inflight = currInflight;
                    foundReady = node;
                }
            } else if (connectionStates.isPreparingConnection(node.idString())) {
                foundConnecting = node;
            } else if (canConnect(node, now)) {
                if (foundCanConnect == null ||
                        this.connectionStates.lastConnectAttemptMs(foundCanConnect.idString()) >
                                this.connectionStates.lastConnectAttemptMs(node.idString())) {
                    foundCanConnect = node;
                }
            } else {
                log.trace("Removing node {} from least loaded node selection since it is neither ready " +
                        "for sending or connecting", node);
            }
        }

        // We prefer established connections if possible. Otherwise, we will wait for connections
        // which are being established before connecting to new nodes.
        if (foundReady != null) {
            log.trace("Found least loaded node {} with {} inflight requests", foundReady, inflight);
            return foundReady;
        } else if (foundConnecting != null) {
            log.trace("Found least loaded connecting node {}", foundConnecting);
            return foundConnecting;
        } else if (foundCanConnect != null) {
            log.trace("Found least loaded node {} with no active connection", foundCanConnect);
            return foundCanConnect;
        } else {
            log.trace("Least loaded node selection failed to find an available node");
            return null;
        }
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        try {
            return AbstractResponse.parseResponse(responseBuffer, requestHeader);
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing response for request with header " + requestHeader, e);
        } catch (CorrelationIdMismatchException e) {
            if (SaslClientAuthenticator.isReserved(requestHeader.correlationId())
                && !SaslClientAuthenticator.isReserved(e.responseCorrelationId()))
                throw new SchemaException("The response is unrelated to Sasl request since its correlation id is "
                    + e.responseCorrelationId() + " and the reserved range for Sasl request is [ "
                    + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + ","
                    + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]");
            else {
                throw e;
            }
        }
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     * @param disconnectState The state of the disconnected channel
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState) {
        processDisconnection(responses, nodeId, now, disconnectState, false);
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    private void processTimeoutDisconnection(List<ClientResponse> responses, String nodeId, long now) {
        processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE, true);
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     * @param disconnectState The state of the disconnected channel
     * @param timedOut {@code true} if the connection is disconnected because of a timeout (request or connection)
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState,
                                      boolean timedOut) {
        connectionStates.disconnected(nodeId, now);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
        switch (disconnectState.state()) {
            case AUTHENTICATION_FAILED:
                AuthenticationException exception = disconnectState.exception();
                connectionStates.authenticationFailed(nodeId, now, exception);
                log.error("Connection to node {} ({}) failed authentication due to: {}", nodeId,
                    disconnectState.remoteAddress(), exception.getMessage());
                break;
            case AUTHENTICATE:
                log.warn("Connection to node {} ({}) terminated during authentication. This may happen " +
                    "due to any of the following reasons: (1) Authentication failed due to invalid " +
                    "credentials with brokers older than 1.0.0, (2) Firewall blocking Kafka TLS " +
                    "traffic (eg it may only allow HTTPS traffic), (3) Transient network issue.",
                    nodeId, disconnectState.remoteAddress());
                break;
            case NOT_CONNECTED:
                log.warn("Connection to node {} ({}) could not be established. Broker may not be available.", nodeId, disconnectState.remoteAddress());
                break;
            default:
                break; // Disconnections in other states are logged at debug level in Selector
        }

        cancelInFlightRequests(nodeId, now, responses, timedOut);
        metadataUpdater.handleServerDisconnect(now, nodeId, Optional.ofNullable(disconnectState.exception()));
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        /**
         * 遍历inFlightRequests，获取超时的node Id列表，循环处理。逻辑和handleDisconnections()一样。
         */
        List<String> nodeIds = this.inFlightRequests.nodesWithTimedOutRequests(now);
        for (String nodeId : nodeIds) {
            // close connection to the node
            this.selector.close(nodeId);
            log.info("Disconnecting from node {} due to request timeout.", nodeId);
            processTimeoutDisconnection(responses, nodeId, now);
        }
    }

    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * Handle socket channel connection timeout. The timeout will hit iff a connection
     * stays at the ConnectionState.CONNECTING state longer than the timeout value,
     * as indicated by ClusterConnectionStates.NodeConnectionState.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutConnections(List<ClientResponse> responses, long now) {
        List<String> nodes = connectionStates.nodesWithConnectionSetupTimeout(now);
        for (String nodeId : nodes) {
            this.selector.close(nodeId);
            log.info(
                "Disconnecting from node {} due to socket connection setup timeout. " +
                "The timeout value is {} ms.",
                nodeId,
                connectionStates.connectionSetupTimeoutMs(nodeId));
            processTimeoutDisconnection(responses, nodeId, now);
        }
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (NetworkSend send : this.selector.completedSends()) {
            InFlightRequest request = this.inFlightRequests.lastSent(send.destinationId());
            if (!request.expectResponse) {
                /**
                 * 如果是不需要响应的请求:
                 * 1、从inFlightRequests中移除该send关联的inFlightRequest
                 * 2、通过InFlightRequest的completed方法，生成ClientResponse。
                 * 3、把ClientResponse放入List<ClientResponse>中
                 */
                this.inFlightRequests.completeLastSent(send.destinationId());
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * If a response from a node includes a non-zero throttle delay and client-side throttling has been enabled for
     * the connection to the node, throttle the connection for the specified delay.
     *
     * @param response the response
     * @param apiVersion the API version of the response
     * @param nodeId the id of the node
     * @param now The current time
     */
    private void maybeThrottle(AbstractResponse response, short apiVersion, String nodeId, long now) {
        int throttleTimeMs = response.throttleTimeMs();
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            connectionStates.throttle(nodeId, now + throttleTimeMs);
            log.trace("Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                      now + throttleTimeMs);
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        /**
         *  handleCompletedReceives()方法
         *  1、从inFlightRequests中移除该NetworkReceive关联的inFlightRequest
         *  2、通过InFlightRequest的completed方法，生成ClientResponse。
         *  3、把ClientResponse放入List<ClientResponse>中
         */
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            //从数据结构里面移除已经接收到响应的请求。
            //把之前存入进去的请求也获取到了
            InFlightRequest req = inFlightRequests.completeNext(source);
            //解析服务端发送回来的请求（里面有响应的结果数据）
            AbstractResponse response = parseResponse(receive.payload(), req.header);
            if (throttleTimeSensor != null)
                throttleTimeSensor.record(response.throttleTimeMs(), now);

            if (log.isDebugEnabled()) {
                log.debug("Received {} response from node {} for request with header {}: {}",
                    req.header.apiKey(), req.destination, req.header, response);
            }

            // If the received response includes a throttle delay, throttle the connection.
            maybeThrottle(response, req.header.apiVersion(), req.destination, now);
            if (req.isInternalRequest && response instanceof MetadataResponse)//如果是关于元数据信息的响应
                //解析完了以后就把封装成一个clientResponse
                //body 存储的是响应的内容
                //req 发送出去的那个请求信息
                metadataUpdater.handleSuccessfulResponse(req.header, now, (MetadataResponse) response);
            else if (req.isInternalRequest && response instanceof ApiVersionsResponse)
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) response);
            else
                responses.add(req.completed(response, now));
        }
    }

    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        final String node = req.destination;
        if (apiVersionsResponse.data().errorCode() != Errors.NONE.code()) {
            if (req.request.version() == 0 || apiVersionsResponse.data().errorCode() != Errors.UNSUPPORTED_VERSION.code()) {
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        Errors.forCode(apiVersionsResponse.data().errorCode()), node, req.header.correlationId());
                this.selector.close(node);
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE);
            } else {
                // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported versions of
                // the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
                // If not provided, the client falls back to version 0.
                short maxApiVersion = 0;
                if (apiVersionsResponse.data().apiKeys().size() > 0) {
                    ApiVersion apiVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
                    if (apiVersion != null) {
                        maxApiVersion = apiVersion.maxVersion();
                    }
                }
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder(maxApiVersion));
            }
            return;
        }
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(
            apiVersionsResponse.data().apiKeys(),
            apiVersionsResponse.data().supportedFeatures(),
            apiVersionsResponse.data().zkMigrationReady());
        apiVersions.update(node, nodeVersionInfo);
        this.connectionStates.ready(node);
        log.debug("Node {} has finalized features epoch: {}, finalized features: {}, supported features: {}, ZK migration ready: {}, API versions: {}.",
                node, apiVersionsResponse.data().finalizedFeaturesEpoch(), apiVersionsResponse.data().finalizedFeatures(),
                apiVersionsResponse.data().supportedFeatures(), apiVersionsResponse.data().zkMigrationReady(), nodeVersionInfo);
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        /**
         * 1、取得node的id和对应的状态
         * 2、processDisconnection中做断开的处理，
         */
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();
            log.info("Node {} disconnected.", node);
            processDisconnection(responses, node, now, entry.getValue());
        }
    }

    /**
     * Record any newly completed connections
     */
    private void handleConnections() {
        /**
         * 1、把需要再次拉取api版本的节点以及对应的ApiVersionsRequest.Builder保存到nodesNeedingApiVersionsFetch
         * 2、把connectionStates中对应的node状态改为已连接
         */
        for (String node : this.selector.connected()) {
            // We are now connected.  Note that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    private void handleInitiateApiVersionRequests(long now) {
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                // We transition the connection to the CHECKING_API_VERSIONS state only when
                // the ApiVersionsRequest is queued up to be sent out. Without this, the client
                // could remain in the CHECKING_API_VERSIONS state forever if the channel does
                // not before ready.
                this.connectionStates.checkingApiVersions(node);
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
                doSend(clientRequest, true, now);
                iter.remove();
            }
        }
    }

    /**
     * Initiate a connection to the given node
     * @param node the node to connect to
     * @param now current time in epoch milliseconds
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            connectionStates.connecting(nodeConnectionId, now, node.host());
            InetAddress address = connectionStates.currentAddress(nodeConnectionId);
            log.debug("Initiating connection to node {} using address {}", node, address);
            //尝试建立连接
            selector.connect(nodeConnectionId,
                    new InetSocketAddress(address, node.port()),
                    this.socketSendBuffer,
                    this.socketReceiveBuffer);
        } catch (IOException e) {
            log.warn("Error connecting to node {}", node, e);
            // Attempt failed, we'll try again after the backoff
            connectionStates.disconnected(nodeConnectionId, now);
            // Notify metadata updater of the connection failure
            metadataUpdater.handleServerDisconnect(now, nodeConnectionId, Optional.empty());
        }
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        // Defined if there is a request in progress, null otherwise
        private InProgressData inProgress;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.inProgress = null;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return !hasFetchInProgress() && this.metadata.timeToNextUpdate(now) == 0;
        }

        private boolean hasFetchInProgress() {
            return inProgress != null;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            long waitForMetadataFetch = hasFetchInProgress() ? defaultRequestTimeoutMs : 0;

            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {
                return metadataTimeout;
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            Node node = leastLoadedNode(now);
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            return maybeUpdate(now, node);
        }

        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            Cluster cluster = metadata.fetch();
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where a connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destinationId);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            // If we have a disconnect while an update is due, we treat it as a failed update
            // so that we can backoff properly
            if (isUpdateDue(now))
                handleFailedRequest(now, Optional.empty());

            maybeFatalException.ifPresent(metadata::fatalError);

            // The disconnect may be the result of stale metadata, so request an update
            metadata.requestUpdate();
        }

        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            maybeFatalException.ifPresent(metadata::fatalError);
            metadata.failedUpdate(now);
            inProgress = null;
        }

        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            // If any partition has leader with missing listeners, log up to ten of these partitions
            // for diagnosing broker configuration issues.
            // This could be a transient issue if listeners were added dynamically to brokers.
            List<TopicPartition> missingListenerPartitions = response.topicMetadata().stream().flatMap(topicMetadata ->
                topicMetadata.partitionMetadata().stream()
                    .filter(partitionMetadata -> partitionMetadata.error == Errors.LISTENER_NOT_FOUND)
                    .map(partitionMetadata -> new TopicPartition(topicMetadata.topic(), partitionMetadata.partition())))
                .collect(Collectors.toList());
            if (!missingListenerPartitions.isEmpty()) {
                int count = missingListenerPartitions.size();
                log.warn("{} partitions have leader brokers without a matching listener, including {}",
                        count, missingListenerPartitions.subList(0, Math.min(10, count)));
            }

            // Check if any topic's metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", requestHeader.correlationId(), errors);

            // When talking to the startup phase of a broker, it is possible to receive an empty metadata set, which
            // we should retry later.
            if (response.brokers().isEmpty()) {
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now);
            } else {
                this.metadata.update(inProgress.requestVersion, response, inProgress.isPartialUpdate, now);
            }

            inProgress = null;
        }

        @Override
        public void close() {
            this.metadata.close();
        }

        /**
         * Return true if there's at least one connection establishment is currently underway
         */
        private boolean isAnyNodeConnecting() {
            for (Node node : fetchNodes()) {
                if (connectionStates.isConnecting(node.idString())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            if (canSendRequest(nodeConnectionId, now)) {
                Metadata.MetadataRequestAndVersion requestAndVersion = metadata.newMetadataRequestAndVersion(now);
                MetadataRequest.Builder metadataRequest = requestAndVersion.requestBuilder;
                log.debug("Sending metadata request {} to node {}", metadataRequest, node);
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                inProgress = new InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate);
                return defaultRequestTimeoutMs;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            if (isAnyNodeConnecting()) {
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                return reconnectBackoffMs;
            }

            if (connectionStates.canConnect(nodeConnectionId, now)) {
                // We don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node);
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            return Long.MAX_VALUE;
        }

        public class InProgressData {
            public final int requestVersion;
            public final boolean isPartialUpdate;

            private InProgressData(int requestVersion, boolean isPartialUpdate) {
                this.requestVersion = requestVersion;
                this.isPartialUpdate = isPartialUpdate;
            }
        }

    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
    }

    // visible for testing
    int nextCorrelationId() {
        if (SaslClientAuthenticator.isReserved(correlation)) {
            // the numeric overflow is fine as negative values is acceptable
            correlation = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + 1;
        }
        return correlation++;
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, nextCorrelationId(), clientId, createdTimeMs, expectResponse,
                requestTimeoutMs, callback);
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }

    static class InFlightRequest {
        final RequestHeader header;
        final String destination;
        final RequestCompletionHandler callback;
        final boolean expectResponse;
        final AbstractRequest request;
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;
        final long createdTimeMs;
        final long requestTimeoutMs;

        public InFlightRequest(ClientRequest clientRequest,
                               RequestHeader header,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this(header,
                 clientRequest.requestTimeoutMs(),
                 clientRequest.createdTimeMs(),
                 clientRequest.destination(),
                 clientRequest.callback(),
                 clientRequest.expectResponse(),
                 isInternalRequest,
                 request,
                 send,
                 sendTimeMs);
        }

        public InFlightRequest(RequestHeader header,
                               int requestTimeoutMs,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.requestTimeoutMs = requestTimeoutMs;
            this.createdTimeMs = createdTimeMs;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
        }

        public long timeElapsedSinceSendMs(long currentTimeMs) {
            return Math.max(0, currentTimeMs - sendTimeMs);
        }

        public long timeElapsedSinceCreateMs(long currentTimeMs) {
            return Math.max(0, currentTimeMs - createdTimeMs);
        }

        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    false, null, null, response);
        }

        public ClientResponse timedOut(long timeMs) {
            // A timed out request is considered disconnected as well
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, true, null, null, null);
        }

        public ClientResponse disconnected(long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, null, null, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }
    }

}
