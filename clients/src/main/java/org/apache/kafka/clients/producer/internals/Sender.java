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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private final Logger log;

    /* the state of each nodes connection */
    private final KafkaClient client;

    /* the record accumulator that batches records */
    private final RecordAccumulator accumulator;

    /* the metadata for the client */
    private final ProducerMetadata metadata;

    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeoutMs;

    /* The max time to wait before retrying a request which has failed */
    private final long retryBackoffMs;

    /* current request API versions supported by the known brokers */
    private final ApiVersions apiVersions;

    /* all the state related to transactions, in particular the producer id, producer epoch, and sequence numbers */
    private final TransactionManager transactionManager;

    // A per-partition queue of batches ordered by creation time for tracking the in-flight batches
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    public Sender(LogContext logContext,
                  KafkaClient client,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager,
                  ApiVersions apiVersions) {
        this.log = logContext.logger(Sender.class);
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
    }

    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    private void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            batches.remove(batch);
            if (batches.isEmpty()) {
                inFlightBatches.remove(batch.topicPartition);
            }
        }
    }

    private void maybeRemoveAndDeallocateBatch(ProducerBatch batch) {
        maybeRemoveFromInflightBatches(batch);
        //看起来这个代码就是要回收资源的。
        this.accumulator.deallocate(batch);
    }

    /**
     *  Get the in-flight batches that has reached delivery timeout.
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();

        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext();) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();
            if (partitionInFlightBatches != null) {
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        iter.remove();
                        // expireBatches is called in Sender.sendProducerData, before client.poll.
                        // The !batch.isDone() invariant should always hold. An IllegalStateException
                        // exception will be thrown if the invariant is violated.
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
                if (partitionInFlightBatches.isEmpty()) {
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            List<ProducerBatch> inflightBatchList = inFlightBatches.get(batch.topicPartition);
            if (inflightBatchList == null) {
                inflightBatchList = new ArrayList<>();
                inFlightBatches.put(batch.topicPartition, inflightBatchList);
            }
            inflightBatchList.add(batch);
        }
    }

    public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
        for (List<ProducerBatch> batchList : batches.values()) {
            addToInflightBatches(batchList);
        }
    }

    private boolean hasPendingTransactionalRequests() {
        return transactionManager != null && transactionManager.hasPendingRequests() && transactionManager.hasOngoingTransaction();
    }

    /**
     * The main run loop for the sender thread
     */
    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        if (transactionManager != null)
            transactionManager.setPoisonStateOnInvalidTransition(true);

        // main loop, runs until close is called
        while (running) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the transaction manager, accumulator or waiting for acknowledgment,
        // wait until these are completed.
        while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // Abort the transaction if any commit or abort didn't go through the transaction manager's queue
        while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
            if (!transactionManager.isCompleting()) {
                log.info("Aborting incomplete transaction due to shutdown");

                try {
                    // It is possible for the transaction manager to throw errors when aborting. Catch these
                    // so as not to interfere with the rest of the shutdown logic.
                    transactionManager.beginAbort();
                } catch (Exception e) {
                    log.error("Error in kafka producer I/O thread while aborting transaction: ", e);
                }
            }
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        if (forceClose) {
            // We need to fail all the incomplete transactional requests and batches and wake up the threads waiting on
            // the futures.
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown");
                transactionManager.close();
            }
            log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     *
     */
    void runOnce() {
        if (transactionManager != null) {
            try {
                transactionManager.maybeResolveSequences();

                RuntimeException lastError = transactionManager.lastError();

                // do not continue sending if the transaction manager is in a failed state
                if (transactionManager.hasFatalError()) {
                    if (lastError != null)
                        maybeAbortBatches(lastError);
                    client.poll(retryBackoffMs, time.milliseconds());
                    return;
                }

                if (transactionManager.hasAbortableError() && shouldHandleAuthorizationError(lastError)) {
                    return;
                }

                // Check whether we need a new producerId. If so, we will enqueue an InitProducerId
                // request which will be sent below
                transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();

                if (maybeSendAndPollTransactionalRequest()) {
                    return;
                }
            } catch (AuthenticationException e) {
                // This is already logged as error, but propagated here to perform any clean ups.
                log.trace("Authentication exception while processing transactional request", e);
                transactionManager.authenticationFailed(e);
            }
        }

        // 1. 获取当前时间的时间戳。
        long currentTimeMs = time.milliseconds();
        // 从accumulator的Deque中取数据，通过client异步发送
        // 2. 调用 sendProducerData 发送消息,但并非真正的发送，而是把消息缓存在 KafkaChannel 的 Send 字段里。下一篇会讲解 NetworkClient。
        long pollTimeout = sendProducerData(currentTimeMs);
        // NIO Reactor事件循环，网络事件发生时，处理网络事件
        // 3. 读取消息实现真正的网络发送
        client.poll(pollTimeout, currentTimeMs);
    }

    // We handle {@code TransactionalIdAuthorizationException} and {@code ClusterAuthorizationException} by first
    // failing the inflight requests, then transition the state to UNINITIALIZED so that the user doesn't need to
    // instantiate the producer again.
    private boolean shouldHandleAuthorizationError(RuntimeException exception) {
        if (exception instanceof TransactionalIdAuthorizationException ||
                        exception instanceof ClusterAuthorizationException) {
            transactionManager.failPendingRequests(new AuthenticationException(exception));
            maybeAbortBatches(exception);
            transactionManager.transitionToUninitialized(exception);
            return true;
        }
        return false;
    }

    /**
     * 最后总结一下Sender从RecordAccmulator中抽取数据的流程
     *
     * 1、执行ready()方法，根据RecordAccumulator的缓存情况，选出可以向哪些Node节点发送消息，返回ReadyCheckResult对象，如果ReadyCheckResult中标识有unknownLeaderTopics，则调用Metadata的requestUpdate方法，标记需要更新Kafka的集群信息。
     * 2、针对ReadyCheckResult中ready node集合，循环调用NetworkClient.ready()方法，目的是检查网络方面是否符合发送消息的条件，不符合条件的Node将从readyNodes中移除。
     * 3、根据以上步骤处理后取得的ready node集合，调用RecordAccumulator.drain()方法，得到每个Node对应的多个ProducerBatchs，就是把原先RecordAccmulator中存储的每个topicPartition对应的ProducerBatch转换成了每个Broker Node可以发送的ProducerBatch，
     * 这样可以把同属于一个Broker的topicPartition的ProducerBatch放到一个ClientRequest中。
     *
     * 3.1.2 准备发送数据
     *
     * @param now
     * @return
     */
    private long sendProducerData(long now) {
        // get the list of partitions with data ready to send
        /**
         * 遍历所有的batch，判断可以发送的batch和获取可以发送batch对应的partition leader
         *
         * 遍历所有队列队首，得到可发送的batch所在的节点集合（并且是leader可知的）返回。
         *
         * 遍历所有的 tp（topic-partition），如果其对应的 RecordBatch 可以发送（大小达到 batch.size 大小或时间达到 linger.ms），就将其对应的 leader 选出来，
         * 最后会返回一个可以发送 Produce request 的 Set<Node>（实际返回的是 ReadyCheckResult 实例，不过 Set<Node> 是最主要的成员变量）；
         *
         */
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(metadata, now);

        // if there are any partitions whose leaders are not known yet, force metadata update
        /**
         * 元数据信息校验：判断需要发送的数据，是否有哪一个partition没有设置leader，如果有需要强制更新
         */
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics)
                // 添加 topic 到没有拉取到元数据的 topic 集合中，并标识需要更新元数据
                this.metadata.add(topic, now);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
                result.unknownLeaderTopics);
            // 这里只完成了 needFullUpdate 字段设置为true，并没有进行真正的获取元数据信息
            this.metadata.requestUpdate();
        }

        // remove any nodes we aren't ready to send to
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            /**
             * 检测要发送的数据网络是否已经建立好，连接建立成功返回true
             *
             * 检查是否能向此node发送消息（这里会保证等待同一node的上次request send发送完成）
             */
            if (!this.client.ready(node, now)) {
                // Update just the readyTimeMs of the latency stats, so that it moves forward
                // every time the batch is ready (then the difference between readyTimeMs and
                // drainTimeMs would represent how long data is waiting for the node).
                // 如果网络连接状态没有建立好
                this.accumulator.updateNodeLatencyStats(node.id(), now, false);
                // 移除掉result中需要发送给broker的数据
                iter.remove();
                // 获得disconnected状态的node剩余backoff时间
                notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
            } else {
                // Update both readyTimeMs and drainTimeMs, this would "reset" the node
                // latency.
                this.accumulator.updateNodeLatencyStats(node.id(), now, true);
            }
        }

        // create produce requests
        /**
         *
         * drain() 是用来遍历可发送请求的 node，然后再遍历在这个 node 拥有leader的分区信息，遍历这些leader分区，从每个leader分区的Deque中使用deque.peekFirst()提取第一个ProducerBatch，
         * 直到超过一个请求的最大长度（max.request.size）为止
         *
         * 最终返回的类型为 Map<Integer, List<RecordBatch>>，key 为 leader.id，value 为要发送的 RecordBatch 的列表，为了减少网络请求的开销，将同一个Node节点的数据聚合在一起发送
         *
         * 如果该 tp 对应的 RecordBatch 不在 backoff 期间（没有重试过，或者重试了但是间隔已经达到了 retryBackoffMs ），
         *
         */
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(metadata, result.readyNodes, this.maxRequestSize, now);
        // 将从消息累加器中读取的数据集，放入正在执行发送inFlightBatches消息批次集合中
        addToInflightBatches(batches);
        //max.in.flight.requests.per.connection = 1: 代表需要保证消息顺序
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            // 发送前，将要发送的TopicPartition放到mute列表中，禁止后续再发送mute列表中的TopicPartition的Batch，避免网络乱序
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    //  对 topicPartition 进行 mute，保证只有一个 batch 正在发送
                /**
                 * 1、mutePartition操作将TopicPartition添加到Set<TopicPartition> muted(静默)集合中
                 * 2、RecordAccumulator提供boolean isMuted(TopicPartition tp)方法可以判断TopicPartition是否是mute
                 * 3、batchReady和drainBatchesForOneNode在提取发送ProducerBatch时对于mute的分区是忽略的
                 */
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }
        // 重置accumulator下一批次的最早过期时间，因为accumulator中提取一部分ProducerBatch，所以最早过期时间需要重新计算
        // getExpiredInflightBatches(now)和this.accumulator.expiredBatches(now)流程中会进行过期时间计算
        accumulator.resetNextBatchExpiryTime();
        // 从正在执行发送数据集合 inflightBatches 中获取过期集合
        // 过期batch会从inflightBatches移除，不能进行发送，而是通知客户端发送失败
        List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
        /**
         * 从accumulator中取出过期的batch，过期batch会从accumulator中移除，不能进行发送，而是通知客户端发送失败
         * batch过期判断：当前时间 - batch创建时间 > delivery.timeout.ms
         *
         * delivery.timeout.ms：由于Send方法并不会立即发送数据，而是先写入缓冲队列，为了保证数据不会一直滞留不发送，
         * 可以配置这个超时时间，过期的batch会被取消发送，同时反馈失败给方法返回的Future对象
         */
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
        // 将发送时间超时的列表加入到过期列表中
        // 从 inflightBatches 与 batches 中查找已过期的消息批次(ProducerBatch)，判断批次是否过期
        // 是根据系统当前时间与 ProducerBatch 创建时间之差是否超过120s，过期时间可以通过参数 delivery.timeout.ms 设置
        expiredBatches.addAll(expiredInflightBatches);

        // Reset the producer id if an expired batch has previously been sent to the broker. Also update the metrics
        // for expired batches. see the documentation of @TransactionState.resetIdempotentProducerId to understand why
        // we need to reset the producer id here.
        // 如果过期批次不为空 则输出对应日志
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        // 处理已过期的消息批次，通知该批消息发送失败并返回给客户端
        for (ProducerBatch expiredBatch : expiredBatches) {
            // 处理当前过期ProducerBatch的回调结果 ProduceRequestResult,并且设置超时异常 new TimeoutException(errorMessage)
            String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
            // 通知该批消息发送失败并返回给客户端
            failBatch(expiredBatch, new TimeoutException(errorMessage), false);
            if (transactionManager != null && expiredBatch.inRetry()) {
                // This ensures that no new batches are drained until the current in flight batches are fully resolved.
                transactionManager.markSequenceUnresolved(expiredBatch);
            }
        }
        // 收集统计指标，后续会专门对 Kafka 的 Metrics 进行分析
        sensors.updateProduceRequestMetrics(batches);

        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout will be the smaller value between next batch expiry
        // time, and the delay time for checking data availability. Note that the nodes may have data that isn't yet
        // sendable due to lingering, backing off, etc. This specifically does not include nodes with sendable data
        // that aren't ready to send since they would cause busy looping.
        // 设置下一次的发送延时

        // 如果存在可发送数据并且准备好发送的node，会以0的timeout来返回pollTimeout，否则此timeout会由当前无法发送消息的nodes决定（比如 lingering, backing off）。总的来说：
        // 1. 如果存在partition的数据准备好发送，pollTimeout=0
        // 2. 另外如果存在partition有数据写入accumulator但是没有准备好发送，pollTimeout=min{当前时间与linger过期时间差值(重发的batch考虑retryBackoff差值), 当前时间与disconnected node的reconnectBackoffMs的时间差值}
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
        pollTimeout = Math.max(pollTimeout, 0);
        // pollTimeout 用来说明下一次拉取需要间隔多久，如果还有ready的数据，会马上进行发送
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            // if some partitions are already ready to be sent, the select time would be 0;
            // otherwise if some partition already has some data accumulated but not ready yet,
            // the select time will be the time difference between now and its linger expiry time;
            // otherwise the select time will be the time difference between now and the metadata expiry time;
            pollTimeout = 0;
        }
        /**
         * 封装请求，通过NetworkClient发送
         *
         * 将同一个broker上面的数据打包到一起通过NetworkClient发送
         * 在集群里面资源是非常宝贵的，如果每一个partition进行一次网络请求，那么太过于繁琐也浪费资源
         * 所以进行了一个打包
         * 里面封装了事务Id 调用了 client.send(clientRequest, now); 通过NetworkClient进行了发送
         *
         * 发送消息暂存到 NetworkClient send 字段里。
         *
         * 将batchs构建成request发送到缓存
         *
         */
        sendProduceRequests(batches, now);
        return pollTimeout;
    }

    /**
     * Returns true if a transactional request is sent or polled, or if a FindCoordinator request is enqueued
     */
    private boolean maybeSendAndPollTransactionalRequest() {
        if (transactionManager.hasInFlightRequest()) {
            // as long as there are outstanding transactional requests, we simply wait for them to return
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        }

        if (transactionManager.hasAbortableError() || transactionManager.isAborting()) {
            if (accumulator.hasIncomplete()) {
                // Attempt to get the last error that caused this abort.
                RuntimeException exception = transactionManager.lastError();
                // If there was no error, but we are still aborting,
                // then this is most likely a case where there was no fatal error.
                if (exception == null) {
                    exception = new TransactionAbortedException();
                }
                accumulator.abortUndrainedBatches(exception);
            }
        }

        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        Node targetNode = null;
        try {
            FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
            targetNode = coordinatorType != null ?
                    transactionManager.coordinator(coordinatorType) :
                    client.leastLoadedNode(time.milliseconds());
            if (targetNode != null) {
                if (!awaitNodeReady(targetNode, coordinatorType)) {
                    log.trace("Target node {} not ready within request timeout, will retry when node is ready.", targetNode);
                    maybeFindCoordinatorAndRetry(nextRequestHandler);
                    return true;
                }
            } else if (coordinatorType != null) {
                log.trace("Coordinator not known for {}, will retry {} after finding coordinator.", coordinatorType, requestBuilder.apiKey());
                maybeFindCoordinatorAndRetry(nextRequestHandler);
                return true;
            } else {
                log.trace("No nodes available to send requests, will poll and retry when until a node is ready.");
                transactionManager.retry(nextRequestHandler);
                client.poll(retryBackoffMs, time.milliseconds());
                return true;
            }

            if (nextRequestHandler.isRetry())
                time.sleep(nextRequestHandler.retryBackoffMs());

            long currentTimeMs = time.milliseconds();
            ClientRequest clientRequest = client.newClientRequest(targetNode.idString(), requestBuilder, currentTimeMs,
                true, requestTimeoutMs, nextRequestHandler);
            log.debug("Sending transactional request {} to node {} with correlation ID {}", requestBuilder, targetNode, clientRequest.correlationId());
            client.send(clientRequest, currentTimeMs);
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        } catch (IOException e) {
            log.debug("Disconnect from {} while trying to send request {}. Going " +
                    "to back off and retry.", targetNode, requestBuilder, e);
            // We break here so that we pick up the FindCoordinator request immediately.
            maybeFindCoordinatorAndRetry(nextRequestHandler);
            return true;
        }
    }

    private void maybeFindCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
        if (nextRequestHandler.needsCoordinator()) {
            transactionManager.lookupCoordinator(nextRequestHandler);
        } else {
            // For non-coordinator requests, sleep here to prevent a tight loop when no node is available
            time.sleep(retryBackoffMs);
            metadata.requestUpdate();
        }

        transactionManager.retry(nextRequestHandler);
    }

    private void maybeAbortBatches(RuntimeException exception) {
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception);
            accumulator.abortBatches(exception);
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    public boolean isRunning() {
        return running;
    }

    private boolean awaitNodeReady(Node node, FindCoordinatorRequest.CoordinatorType coordinatorType) throws IOException {
        if (NetworkClientUtils.awaitReady(client, node, time, requestTimeoutMs)) {
            if (coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
                // Indicate to the transaction manager that the coordinator is ready, allowing it to check ApiVersions
                // This allows us to bump transactional epochs even if the coordinator is temporarily unavailable at
                // the time when the abortable error is handled
                transactionManager.handleCoordinatorReady();
            }
            return true;
        }
        return false;
    }

    /**
     * Handle a produce response
     *
     * 按照不同的情况分别调用completeBatch方法
     * 1、连接被关闭
     * 2、version不匹配
     * 3、正常情况
     * 取得ProduceResponse，迭代它内部的Map<TopicPartition, PartitionResponse> responses
     * 3.1取主题分区
     * 3.2根据主题分区，取得该主题分区下的所有batches
     * 3.3调用completeBatch
     *
     * 由此可以见正常情况，是按照主题分区分别调用completeBatch
     *
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
        RequestHeader requestHeader = response.requestHeader();
        int correlationId = requestHeader.correlationId();
        if (response.wasTimedOut()) {
            log.trace("Cancelled request with header {} due to the last request to node {} timed out",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.REQUEST_TIMED_OUT, String.format("Disconnected from node %s due to timeout", response.destination())),
                        correlationId, now);
        } else if (response.wasDisconnected()) {
            // 处理连接断开batches
            //这个地方就是就是一个特殊情况，发现 broker失去连接，不过这个是一个小概率事件。
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION, String.format("Disconnected from node %s", response.destination())),
                        correlationId, now);
        } else if (response.versionMismatch() != null) {
            // 处理协议版本不匹配
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now);
        } else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            //正常情况下，走的都是这个分支
            // 如果response有内容，则解析后completeBatch
            if (response.hasResponse()) {
                // Sender should exercise PartitionProduceResponse rather than ProduceResponse.PartitionResponse
                // https://issues.apache.org/jira/browse/KAFKA-10696
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                /**
                 * 遍历每个分区的响应
                 */
                produceResponse.data().responses().forEach(r -> r.partitionResponses().forEach(p -> {
                    TopicPartition tp = new TopicPartition(r.name(), p.index());
                    ProduceResponse.PartitionResponse partResp = new ProduceResponse.PartitionResponse(
                            Errors.forCode(p.errorCode()),
                            p.baseOffset(),
                            p.logAppendTimeMs(),
                            p.logStartOffset(),
                            p.recordErrors()
                                .stream()
                                .map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage()))
                                .collect(Collectors.toList()),
                            p.errorMessage());
                    //获取到当前分区的响应
                    ProducerBatch batch = batches.get(tp);
                    //对响应进行处理
                    completeBatch(batch, partResp, correlationId, now);
                }));
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // this is the acks = 0 case, just complete all requests
                // 如果没有response，即ack = 0 时会走此处逻辑，直接completeBatch
                //acks=0意思就是不需要返回响应，在生产环境里面，我们一般是不会把acks 参数设置为0
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     *
     * 这是请求最终完成返回处理的方法，逻辑如下：
     * 按照不同情况进行处理
     * 1、如果是batch太大，那么对batch进行切分，然后重新通过追加消息到RecordAccumulator中
     * 2、如果有error，并且可以重试，则再次通过追加消息到RecordAccumulator中
     * 3、如果没有error，则调用completeBatch(batch, response)，调用batch的回调方法，最后释放空间。
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now) {
        //如果处理成功那就是成功了，但是如果服务端那儿处理失败了，是不是也要给我们发送回来异常的信息。
        //error 这个里面存储的就是服务端发送回来的异常码
        Errors error = response.error;


        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // If the batch is too large, we split the batch and send the split batches again. We do not decrement
            // the retry attempts in this case.
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                this.retries - batch.attempts(),
                formatErrMsg(response));
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            this.accumulator.splitAndReenqueue(batch);
            maybeRemoveAndDeallocateBatch(batch);
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) {
            if (canRetry(batch, response, now)) {
                //如果响应里面带有异常 并且 这个请求是可以重试的
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts() - 1,
                    formatErrMsg(response));
                /**
                 * 重新把发送失败等着批次 加入到队列里面。
                 * 重试的Batch会放入到队列的头部，不是尾部，这样的话，下一次循环的时候就可以优先处理这个要重新发送的Batch了，
                 * attempts、lastAttemptMs这些参数都会进行设置，辅助判断这个Batch下一次是什么时候要进行重试发送
                 */
                reenqueueBatch(batch, now);
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number has advanced beyond
                // the sequence of the current batch, and we haven't retained batch metadata on the broker to return
                // the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid offset and timestamp.
                completeBatch(batch, response);
            } else {
                // tell the user the result of their request. We only adjust sequence numbers if the batch didn't exhaust
                // its retries -- if it did, we don't know whether the sequence number was accepted or not, and
                // thus it is not safe to reassign the sequence.
                failBatch(batch, response, batch.attempts() < this.retries);
            }
            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                            "topic-partition may not exist or the user may not have Describe access to it",
                        batch.topicPartition);
                } else {
                    log.warn("Received invalid metadata error in produce request on partition {} due to {}. Going " +
                            "to request metadata update now", batch.topicPartition,
                            error.exception(response.errorMessage).toString());
                }
                metadata.requestUpdate();
            }
        } else {
            //里面调用了用户传进来的回调函数，回调函数调用了以后，说明我们的一个完整的消息的发送流程就结束了。
            completeBatch(batch, response);
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * Format the error from a {@link ProduceResponse.PartitionResponse} in a user-friendly string
     * e.g "NETWORK_EXCEPTION. Error Message: Disconnected from node 0"
     */
    private String formatErrMsg(ProduceResponse.PartitionResponse response) {
        String errorMessageSuffix = (response.errorMessage == null || response.errorMessage.isEmpty()) ?
                "" : String.format(". Error Message: %s", response.errorMessage);
        return String.format("%s%s", response.error, errorMessageSuffix);
    }

    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        this.accumulator.reenqueue(batch, currentTimeMs);
        maybeRemoveFromInflightBatches(batch);
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        if (transactionManager != null) {
            transactionManager.handleCompletedBatch(batch, response);
        }

        if (batch.complete(response.baseOffset, response.logAppendTime)) {
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    private void failBatch(ProducerBatch batch,
                           ProduceResponse.PartitionResponse response,
                           boolean adjustSequenceNumbers) {
        final RuntimeException topLevelException;
        if (response.error == Errors.TOPIC_AUTHORIZATION_FAILED)
            topLevelException = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
        else if (response.error == Errors.CLUSTER_AUTHORIZATION_FAILED)
            topLevelException = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
        else
            topLevelException = response.error.exception(response.errorMessage);

        if (response.recordErrors == null || response.recordErrors.isEmpty()) {
            failBatch(batch, topLevelException, adjustSequenceNumbers);
        } else {
            Map<Integer, RuntimeException> recordErrorMap = new HashMap<>(response.recordErrors.size());
            for (ProduceResponse.RecordError recordError : response.recordErrors) {
                // The API leaves us with some awkwardness interpreting the errors in the response.
                // We cannot differentiate between different error cases (such as INVALID_TIMESTAMP)
                // from the single error code at the partition level, so instead we use INVALID_RECORD
                // for all failed records and rely on the message to distinguish the cases.
                final String errorMessage;
                if (recordError.message != null) {
                    errorMessage = recordError.message;
                } else if (response.errorMessage != null) {
                    errorMessage = response.errorMessage;
                } else {
                    errorMessage = response.error.message();
                }

                // If the batch contained only a single record error, then we can unambiguously
                // use the exception type corresponding to the partition-level error code.
                if (response.recordErrors.size() == 1) {
                    recordErrorMap.put(recordError.batchIndex, response.error.exception(errorMessage));
                } else {
                    recordErrorMap.put(recordError.batchIndex, new InvalidRecordException(errorMessage));
                }
            }

            Function<Integer, RuntimeException> recordExceptions = batchIndex -> {
                RuntimeException exception = recordErrorMap.get(batchIndex);
                if (exception != null) {
                    return exception;
                } else {
                    // If the response contains record errors, then the records which failed validation
                    // will be present in the response. To avoid confusion for the remaining records, we
                    // return a generic exception.
                    return new KafkaException("Failed to append record because it was part of a batch " +
                        "which had one more more invalid records");
                }
            };

            failBatch(batch, topLevelException, recordExceptions, adjustSequenceNumbers);
        }
    }

    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        boolean adjustSequenceNumbers
    ) {
        failBatch(batch, topLevelException, batchIndex -> topLevelException, adjustSequenceNumbers);
    }

    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions,
        boolean adjustSequenceNumbers
    ) {
        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        if (batch.completeExceptionally(topLevelException, recordExceptions)) {
            if (transactionManager != null) {
                try {
                    // This call can throw an exception in the rare case that there's an invalid state transition
                    // attempted. Catch these so as not to interfere with the rest of the logic.
                    transactionManager.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers);
                } catch (Exception e) {
                    log.debug("Encountered error when transaction manager was handling a failed batch", e);
                }
            }
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed.
     * We can also retry OutOfOrderSequence exceptions for future batches, since if the first batch has failed, the
     * future batches are certain to fail with an OutOfOrderSequence exception.
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) &&
            batch.attempts() < this.retries &&
            !batch.isDone() &&
            (transactionManager == null ?
                    response.error.exception() instanceof RetriableException :
                    transactionManager.canRetry(response, batch));
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        // 遍历batches，以node为单位发送request请求
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
    }

    /**
     * Create a produce request from the given record batches
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        if (batches.isEmpty())
            return;

        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());

        // find the minimum magic version used when creating the record sets
        byte minUsedMagic = apiVersions.maxUsableProduceMagic();
        for (ProducerBatch batch : batches) {
            if (batch.magic() < minUsedMagic)
                minUsedMagic = batch.magic();
        }
        ProduceRequestData.TopicProduceDataCollection tpd = new ProduceRequestData.TopicProduceDataCollection();
        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            MemoryRecords records = batch.records();

            // down convert if necessary to the minimum magic used. In general, there can be a delay between the time
            // that the producer starts building the batch and the time that we send the request, and we may have
            // chosen the message format based on out-dated metadata. In the worst case, we optimistically chose to use
            // the new message format, but found that the broker didn't support it, so we need to down-convert on the
            // client before sending. This is intended to handle edge cases around cluster upgrades where brokers may
            // not all support the same message format version. For example, if a partition migrates from a broker
            // which is supporting the new magic version to one which doesn't, then we will need to convert.
            if (!records.hasMatchingMagic(minUsedMagic))
                records = batch.records().downConvert(minUsedMagic, 0, time).records();
            ProduceRequestData.TopicProduceData tpData = tpd.find(tp.topic());
            if (tpData == null) {
                tpData = new ProduceRequestData.TopicProduceData().setName(tp.topic());
                tpd.add(tpData);
            }
            tpData.partitionData().add(new ProduceRequestData.PartitionProduceData()
                    .setIndex(tp.partition())
                    .setRecords(records));
            recordsByPartition.put(tp, batch);
        }

        String transactionalId = null;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
        }

        // 用produceRecordsByPartition构建requestBuilder
        ProduceRequest.Builder requestBuilder = ProduceRequest.forMagic(minUsedMagic,
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(timeout)
                        .setTransactionalId(transactionalId)
                        .setTopicData(tpd));
        // 用recordsByPartition构建回调类，client.poll收到response后会调用callback.onComplete方法。
        RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());

        String nodeId = Integer.toString(destination);
        /**
         * 封装请求  参数包含一个回调函数
         * 封装ClientRequest请求参数，他需要包含对应的请求头，api key，api version，acks，request timeout，接着才是请求体，里面就是包含了对应的多个batch的数据，最后的最后，一定是把刚才说的那些东西都给打成一个二进制的字节数组
         * ClientRequest里面就是封装了按照二进制协议的格式，放入了组装好的数据，发送到broker上去的有很多个Topic，每个Topic有很多Partition，每个Partitioin是对应就一个batch的数据发送过去
         *
         */
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
                requestTimeoutMs, callback);
        //调用client.send发送消息
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * A collection of sensors for the sender
     */
    private static class SenderMetrics {
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor batchSplitSensor;
        private final SenderMetricsRegistry metrics;
        private final Time time;

        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            this.metrics.addMetric(metrics.metadataAge,
                (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (ProducerBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Objects.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // global metrics
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);
                    this.compressionRateSensor.record(batch.compressionRatio());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        void recordBatchSplit() {
            this.batchSplitSensor.record();
        }
    }

}
