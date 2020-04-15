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

package software.amazon.kinesis.retrieval;

import java.util.function.Function;
import software.amazon.kinesis.retrieval.polling.DataFetcher;

public interface RetrievalSpecificConfig {
    /**
     * Creates and returns a retrieval factory for the specific configuration
     *
     * @return a retrieval factory that can create an appropriate retriever
     */
    RetrievalFactory retrievalFactory();


    /**
     * Creates and returns a retrieval factory for the specific configuration based on provided data fetcher
     *
     * @return a retrieval factory that can create an appropriate retriever with a given data fetcher
     */
    default RetrievalFactory retrievalFactory(Function<DataFetcherProviderConfig, DataFetcher> dataFetcherProvider) {
        throw new UnsupportedOperationException("RetrievalFactory with custom dataFetcher not supported");
    }
}
