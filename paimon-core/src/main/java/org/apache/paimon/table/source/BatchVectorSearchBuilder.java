/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.paimon.table.source;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Builder to build batch vector search over multiple query vectors. */
public interface BatchVectorSearchBuilder extends Serializable {

    /** Push partition filters. */
    BatchVectorSearchBuilder withPartitionFilter(PartitionPredicate partitionPredicate);

    /** Push pre-filter for vector search. */
    BatchVectorSearchBuilder withFilter(Predicate predicate);

    /** The top k results to return per query vector. */
    BatchVectorSearchBuilder withLimit(int limit);

    /** The vector column to search. */
    BatchVectorSearchBuilder withVectorColumn(String name);

    /** The query vectors; result {@code i} corresponds to {@code vectors[i]}. */
    BatchVectorSearchBuilder withVectors(float[][] vectors);

    /** Pin subsequent vector scan planning and centroid routing to a snapshot. */
    default BatchVectorSearchBuilder withSnapshot(Snapshot snapshot) {
        return this;
    }

    /** Restrict vector search to the given routed IVF centroids. */
    default BatchVectorSearchBuilder withRoutedCentroids(Set<Integer> centroidIds) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support routed IVF centroids.");
    }

    /**
     * Route each query vector to IVF centroid ids, or return {@code null} if routing is
     * unsupported.
     */
    @Nullable
    default List<Set<Integer>> routeCentroids() {
        return routeCentroids(null);
    }

    /**
     * Route each query vector to IVF centroid ids with a caller-scoped routing model cache, or
     * return {@code null} if routing is unsupported.
     */
    @Nullable
    default List<Set<Integer>> routeCentroids(Map<Path, AutoCloseable> routingModelCache) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support routing IVF centroids.");
    }

    /** Option for vector indexes. */
    default BatchVectorSearchBuilder withOption(String key, String value) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support vector options.");
    }

    /** Options for vector indexes. */
    default BatchVectorSearchBuilder withOptions(Map<String, String> options) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support vector options.");
    }

    /** Create vector scan to scan index files. */
    VectorScan newVectorScan();

    /** Create batch vector read to read index files. */
    BatchVectorRead newBatchVectorRead();

    /** Execute batch vector search locally; result {@code i} corresponds to {@code vectors[i]}. */
    default List<GlobalIndexResult> executeBatchLocal() {
        return newBatchVectorRead().readBatch(newVectorScan().scan());
    }
}
