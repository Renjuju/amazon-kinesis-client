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

package software.amazon.kinesis.lifecycle;

import lombok.AllArgsConstructor;
import org.reactivestreams.Subscriber;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RecordsDeliveryAck;

@AllArgsConstructor
public class ShardConsumerNotifyingSubscriber implements NotifyingSubscriber {

    private final Subscriber<RecordsRetrieved> delegate;

    private final RecordsPublisher recordsPublisher;

    @Override
    public Subscriber<RecordsRetrieved> getDelegateSubscriber() {
        return delegate;
    }

    @Override
    public RecordsPublisher getWaitingRecordsPublisher() {
        return recordsPublisher;
    }

    @Override
    public RecordsDeliveryAck getRecordsRetrievedAck(RecordsRetrieved recordsRetrieved) {
        return () -> recordsRetrieved.batchUniqueIdentifier();
    }
}
