package software.amazon.kinesis.retrieval.polling;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

public interface DataFetcher {
    /**
     * Get records from the current position in the stream (up to maxRecords).
     *
     * @return list of records of up to maxRecords size
     */
    DataFetcherResult getRecords();

    /**
     * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number.
     *
     * @param initialCheckpoint       Current checkpoint sequence number for this shard.
     * @param initialPositionInStream The initialPositionInStream.
     */
    void initialize(String initialCheckpoint,
                    InitialPositionInStreamExtended initialPositionInStream);

    void initialize(ExtendedSequenceNumber initialCheckpoint,
                    InitialPositionInStreamExtended initialPositionInStream);

    /**
     * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
     *
     * @param sequenceNumber          advance the iterator to the record at this sequence number.
     * @param initialPositionInStream The initialPositionInStream.
     */
    void advanceIteratorTo(String sequenceNumber,
                           InitialPositionInStreamExtended initialPositionInStream);

    /**
     * Gets a new iterator from the last known sequence number i.e. the sequence number of the last record from the last
     * records call.
     */
    void restartIterator();

    void resetIterator(String shardIterator, String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream);

    GetRecordsResponse getResponse(GetRecordsRequest request) throws ExecutionException, InterruptedException, TimeoutException;

    GetRecordsRequest getRequest(String nextIterator);

    String getNextIterator(GetShardIteratorRequest request) throws ExecutionException, InterruptedException, TimeoutException;

    GetRecordsResponse getRecords(@NonNull String nextIterator);

    software.amazon.kinesis.common.StreamIdentifier getStreamIdentifier();

    boolean isShardEndReached();
}
