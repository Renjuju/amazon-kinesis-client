/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.leases;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseManagementFactory;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.MultiStreamTracker;

/**
 * Used by the KCL to configure lease management.
 */
@Data
@Accessors(fluent = true)
public class LeaseManagementConfig {

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(1);

    /**
     * Name of the table to use in DynamoDB
     *
     * @return String
     */
    @NonNull
    private final String tableName;
    /**
     * Client to be used to access DynamoDB service.
     *
     * @return {@link DynamoDbAsyncClient}
     */
    @NonNull
    private final DynamoDbAsyncClient dynamoDBClient;
    /**
     * Client to be used to access Kinesis Data Streams service.
     *
     * @return {@link KinesisAsyncClient}
     */
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    /**
     * Name of the Kinesis Data Stream to read records from.
     */
    @NonNull
    private String streamName;
    /**
     * Used to distinguish different workers/processes of a KCL application.
     *
     * @return String
     */
    @NonNull
    private final String workerIdentifier;

    /**
     * Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
     * will be regarded as having problems and it's shards will be assigned to other workers.
     * For applications that have a large number of shards, this may be set to a higher number to reduce
     * the number of DynamoDB IOPS required for tracking leases.
     *
     * <p>Default value: 10000L</p>
     */
    private long failoverTimeMillis = 10000L;

    /**
     * Shard sync interval in milliseconds - e.g. wait for this long between shard sync tasks.
     *
     * <p>Default value: 60000L</p>
     */
    private long shardSyncIntervalMillis = 60000L;

    /**
     * Cleanup leases upon shards completion (don't wait until they expire in Kinesis).
     * Keeping leases takes some tracking/resources (e.g. they need to be renewed, assigned), so by default we try
     * to delete the ones we don't need any longer.
     *
     * <p>Default value: true</p>
     */
    private boolean cleanupLeasesUponShardCompletion = true;

    /**
     * The max number of leases (shards) this worker should process.
     * This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
     * or during deployment.
     *
     * <p>NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
     * stream due to the max limit.</p>
     *
     * <p>Default value: {@link Integer#MAX_VALUE}</p>
     */
    private int maxLeasesForWorker = Integer.MAX_VALUE;

    /**
     * Max leases to steal from another worker at one time (for load balancing).
     * Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
     * but can cause higher churn in the system.
     *
     * <p>Default value: 1</p>
     */
    private int maxLeasesToStealAtOneTime = 1;

    /**
     * The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
     *
     * <p>Default value: 10</p>
     */
    private int initialLeaseTableReadCapacity = 10;

    /**
     * The Amazon DynamoDB table used for tracking leases will be provisioned with this write capacity.
     *
     * <p>Default value: 10</p>
     */
    private int initialLeaseTableWriteCapacity = 10;

    /**
     * Configurable function to override the existing shardDetector.
     * This is for all custom shard detector using a KinesisShardDetector,
     * e.g.
     * <pre>
     *    Function<Pair<StreamIdentifier, KinesisShardDetector>, ShardDetector> customShardDetectorProvider =
     *    pair -> new CustomShardDetector(pair.getLeft(), pair.getRight());
     *    // pair contains the StreamIdentifier and KinesisShardDetector used by KCL
     * </pre>
     */
    private Function<Pair<StreamIdentifier, KinesisShardDetector>, ShardDetector> customShardDetectorProvider;

    /**
     * The size of the thread pool to create for the lease renewer to use.
     *
     * <p>Default value: 20</p>
     */
    private int maxLeaseRenewalThreads = 20;

    /**
     *
     */
    private boolean ignoreUnexpectedChildShards = false;

    /**
     *
     */
    private boolean consistentReads = false;

    private long listShardsBackoffTimeInMillis = 1500L;

    private int maxListShardsRetryAttempts = 50;

    public long epsilonMillis = 25L;

    private Duration dynamoDbRequestTimeout = DEFAULT_REQUEST_TIMEOUT;

    private BillingMode billingMode = BillingMode.PROVISIONED;

    /**
     * The initial position for getting records from Kinesis streams.
     *
     * <p>Default value: {@link InitialPositionInStream#TRIM_HORIZON}</p>
     */
    private InitialPositionInStreamExtended initialPositionInStream =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    private int maxCacheMissesBeforeReload = 1000;
    private long listShardsCacheAllowedAgeInSeconds = 30;
    private int cacheMissWarningModulus = 250;

    private MetricsFactory metricsFactory = new NullMetricsFactory();

    @Deprecated
    public LeaseManagementConfig(String tableName, DynamoDbAsyncClient dynamoDBClient, KinesisAsyncClient kinesisClient,
            String streamName, String workerIdentifier) {
        this.tableName = tableName;
        this.dynamoDBClient = dynamoDBClient;
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.workerIdentifier = workerIdentifier;
    }

    public LeaseManagementConfig(String tableName, DynamoDbAsyncClient dynamoDBClient, KinesisAsyncClient kinesisClient,
            String workerIdentifier) {
        this.tableName = tableName;
        this.dynamoDBClient = dynamoDBClient;
        this.kinesisClient = kinesisClient;
        this.workerIdentifier = workerIdentifier;
    }

    /**
     * Returns the metrics factory.
     *
     * <p>
     * NOTE: This method is deprecated and will be removed in a future release. This metrics factory is not being used
     * in the KCL.
     * </p>
     *
     * @return
     */
    @Deprecated
    public MetricsFactory metricsFactory() {
        return metricsFactory;
    }

    /**
     * Sets the metrics factory.
     *
     * <p>
     * NOTE: This method is deprecated and will be removed in a future release. This metrics factory is not being used
     * in the KCL.
     * </p>
     *
     * @param metricsFactory
     */
    @Deprecated
    public LeaseManagementConfig metricsFactory(final MetricsFactory metricsFactory) {
        this.metricsFactory = metricsFactory;
        return this;
    }

    /**
     * The {@link ExecutorService} to be used by {@link ShardSyncTaskManager}.
     *
     * <p>Default value: {@link LeaseManagementThreadPool}</p>
     */
    private ExecutorService executorService = new LeaseManagementThreadPool(
            new ThreadFactoryBuilder().setNameFormat("ShardSyncTaskManager-%04d").build());

    static class LeaseManagementThreadPool extends ThreadPoolExecutor {
        private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;

        LeaseManagementThreadPool(ThreadFactory threadFactory) {
            super(0, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    threadFactory);
        }
    }

    /**
     * Callback used with DynamoDB lease management. Callback is invoked once the table is newly created and is in the
     * active status.
     *
     * <p>
     * Default value: {@link TableCreatorCallback#NOOP_TABLE_CREATOR_CALLBACK}
     * </p>
     */
    private TableCreatorCallback tableCreatorCallback = TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK;

    private HierarchicalShardSyncer hierarchicalShardSyncer;

    private LeaseManagementFactory leaseManagementFactory;

    private HierarchicalShardSyncer hierarchicalShardSyncer() {
        if(hierarchicalShardSyncer == null) {
            hierarchicalShardSyncer = new HierarchicalShardSyncer();
        }
        return hierarchicalShardSyncer;
    }

    /**
     * Vends HierarchicalShardSyncer based on MultiStreamingMode. With MultiStreamMode shard syncer creates
     * leases to accommodate more than one stream.
     * @param isMultiStreamingMode
     * @return HierarchicalShardSyncer
     */
    public HierarchicalShardSyncer hierarchicalShardSyncer(boolean isMultiStreamingMode) {
        if(hierarchicalShardSyncer == null) {
            hierarchicalShardSyncer = new HierarchicalShardSyncer(isMultiStreamingMode);
        }
        return hierarchicalShardSyncer;
    }

    @Deprecated
    public LeaseManagementFactory leaseManagementFactory() {
        if (leaseManagementFactory == null) {
            Validate.notEmpty(streamName(), "Stream name is empty");
            leaseManagementFactory = new DynamoDBLeaseManagementFactory(kinesisClient(),
                        streamName(),
                        dynamoDBClient(),
                        tableName(),
                        workerIdentifier(),
                        executorService(),
                        initialPositionInStream(),
                        failoverTimeMillis(),
                        epsilonMillis(),
                        maxLeasesForWorker(),
                        maxLeasesToStealAtOneTime(),
                        maxLeaseRenewalThreads(),
                        cleanupLeasesUponShardCompletion(),
                        ignoreUnexpectedChildShards(),
                        shardSyncIntervalMillis(),
                        consistentReads(),
                        listShardsBackoffTimeInMillis(),
                        maxListShardsRetryAttempts(),
                        maxCacheMissesBeforeReload(),
                        listShardsCacheAllowedAgeInSeconds(),
                        cacheMissWarningModulus(),
                        initialLeaseTableReadCapacity(),
                        initialLeaseTableWriteCapacity(),
                        hierarchicalShardSyncer(),
                        tableCreatorCallback(), dynamoDbRequestTimeout(), billingMode());
        }
        return leaseManagementFactory;
    }

    /**
     * Vends LeaseManagementFactory that performs serde based on leaseSerializer and shard sync based on isMultiStreamingMode
     * @param leaseSerializer
     * @param isMultiStreamingMode
     * @return LeaseManagementFactory
     */
    public LeaseManagementFactory leaseManagementFactory(final LeaseSerializer leaseSerializer, boolean isMultiStreamingMode) {
        if(leaseManagementFactory == null) {
            leaseManagementFactory = new DynamoDBLeaseManagementFactory(kinesisClient(),
                dynamoDBClient(),
                tableName(),
                workerIdentifier(),
                executorService(),
                failoverTimeMillis(),
                epsilonMillis(),
                maxLeasesForWorker(),
                maxLeasesToStealAtOneTime(),
                maxLeaseRenewalThreads(),
                cleanupLeasesUponShardCompletion(),
                ignoreUnexpectedChildShards(),
                shardSyncIntervalMillis(),
                consistentReads(),
                listShardsBackoffTimeInMillis(),
                maxListShardsRetryAttempts(),
                maxCacheMissesBeforeReload(),
                listShardsCacheAllowedAgeInSeconds(),
                cacheMissWarningModulus(),
                initialLeaseTableReadCapacity(),
                initialLeaseTableWriteCapacity(),
                hierarchicalShardSyncer(isMultiStreamingMode),
                tableCreatorCallback(),
                dynamoDbRequestTimeout(),
                billingMode(),
                leaseSerializer);
        }
        return leaseManagementFactory;
    }

    /**
     * Set leaseManagementFactory and return the current LeaseManagementConfig instance.
     * @param leaseManagementFactory
     * @return LeaseManagementConfig
     */
    public LeaseManagementConfig leaseManagementFactory(final LeaseManagementFactory leaseManagementFactory) {
        this.leaseManagementFactory = leaseManagementFactory;
        return this;
    }

}
