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

    DataFetcherResult getRecords();

    void initialize(String initialCheckpoint,
                    InitialPositionInStreamExtended initialPositionInStream);

    void initialize(ExtendedSequenceNumber initialCheckpoint,
                    InitialPositionInStreamExtended initialPositionInStream);

    void advanceIteratorTo(String sequenceNumber,
                           InitialPositionInStreamExtended initialPositionInStream);

    void restartIterator();

    void resetIterator(String shardIterator,
                       String sequenceNumber,
                       InitialPositionInStreamExtended initialPositionInStream);

    boolean isShardEndReached();

    GetRecordsResponse getResponse(GetRecordsRequest request) throws ExecutionException, InterruptedException, TimeoutException;

    GetRecordsRequest getRequest(@NonNull String nextIterator) throws ExecutionException, InterruptedException, TimeoutException;

    String getNextIterator(GetShardIteratorRequest request) throws ExecutionException, InterruptedException, TimeoutException;
}
