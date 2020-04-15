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

package software.amazon.kinesis.retrieval.polling;

import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;

import java.time.Duration;

/**
 *
 */
@Data
@KinesisClientInternalApi
public class SynchronousBlockingRetrievalFactory implements RetrievalFactory {

    @NonNull
    private final String streamName;
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final RecordsFetcherFactory recordsFetcherFactory;
    // private final long listShardsBackoffTimeInMillis;
    // private final int maxListShardsRetryAttempts;
    private final int maxRecords;
    private final Duration kinesisRequestTimeout;

    private final Function<Pair<KinesisDataFetcher, StreamIdentifier>, DataFetcher> dataFetcherProvider;

    public SynchronousBlockingRetrievalFactory(String streamName,
                                               KinesisAsyncClient kinesisClient,
                                               RecordsFetcherFactory recordsFetcherFactory,
                                               int maxRecords,
                                               Duration kinesisRequestTimeout,
                                               Function<Pair<KinesisDataFetcher, StreamIdentifier>, DataFetcher> dataFetcherProvider) {
        this.streamName = streamName;
        this.kinesisClient = kinesisClient;
        this.recordsFetcherFactory = recordsFetcherFactory;
        this.maxRecords = maxRecords;
        this.kinesisRequestTimeout = kinesisRequestTimeout;
        this.dataFetcherProvider = dataFetcherProvider;
    }

    @Deprecated
    public SynchronousBlockingRetrievalFactory(String streamName, KinesisAsyncClient kinesisClient, RecordsFetcherFactory recordsFetcherFactory, int maxRecords) {
        this(streamName, kinesisClient, recordsFetcherFactory, maxRecords, PollingConfig.DEFAULT_REQUEST_TIMEOUT, null);
    }

    @Override
    public GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(@NonNull final ShardInfo shardInfo,
            @NonNull final MetricsFactory metricsFactory) {
        final StreamIdentifier streamIdentifier = shardInfo.streamIdentifierSerOpt().isPresent() ?
                StreamIdentifier.multiStreamInstance(shardInfo.streamIdentifierSerOpt().get()) :
                StreamIdentifier.singleStreamInstance(streamName);
        KinesisDataFetcher kinesisDataFetcher = new KinesisDataFetcher(kinesisClient,
                streamIdentifier,
                shardInfo.shardId(),
                maxRecords,
                metricsFactory,
                kinesisRequestTimeout);

        DataFetcher dataFetcher = this.dataFetcherProvider == null ? kinesisDataFetcher :
                this.dataFetcherProvider.apply(Pair.of(kinesisDataFetcher, streamIdentifier));

        return new SynchronousGetRecordsRetrievalStrategy(dataFetcher);
    }

    @Override
    public RecordsPublisher createGetRecordsCache(@NonNull final ShardInfo shardInfo,
            @NonNull final MetricsFactory metricsFactory) {
        return recordsFetcherFactory.createRecordsFetcher(createGetRecordsRetrievalStrategy(shardInfo, metricsFactory),
                shardInfo.shardId(), metricsFactory, maxRecords);
    }
}
