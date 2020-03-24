package software.amazon.kinesis.retrieval.polling;

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
}
