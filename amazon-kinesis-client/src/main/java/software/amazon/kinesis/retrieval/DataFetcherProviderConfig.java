package software.amazon.kinesis.retrieval;

import lombok.Data;
import lombok.NonNull;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;


/**
 * Configuration needed for custom data fetchers
 */
@Data
public class DataFetcherProviderConfig {

    @NonNull
    private StreamIdentifier streamIdentifier;

    @NonNull
    private String shardId;

    @NonNull
    private MetricsFactory metricsFactory;

    @NonNull
    private Integer maxRecords;
}
