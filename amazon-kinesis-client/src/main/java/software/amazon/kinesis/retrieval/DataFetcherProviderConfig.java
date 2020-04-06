package software.amazon.kinesis.retrieval;

import lombok.Data;
import lombok.NonNull;
import software.amazon.kinesis.common.StreamIdentifier;


/**
 * Configuration needed for custom data fetchers
 */
@Data
public class DataFetcherProviderConfig {

    @NonNull
    private StreamIdentifier streamIdentifier;

    @NonNull
    private String shardId;
}
