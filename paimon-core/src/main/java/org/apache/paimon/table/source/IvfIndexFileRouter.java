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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.IndexFileKind;
import org.apache.paimon.globalindex.RoutedVectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Planner-side index file router for centroid-sharded IVF vector indexes. */
class IvfIndexFileRouter {

    private static final Logger LOG = LoggerFactory.getLogger(IvfIndexFileRouter.class);

    private static final Set<String> IVF_INDEX_TYPES =
            new HashSet<>(Arrays.asList("ivf-pq", "ivf-flat", "ivf-sq", "ivf-rq"));

    private final FileStoreTable table;
    @Nullable private final Predicate filter;
    private final DataField vectorColumn;
    private final Map<String, String> options;
    @Nullable private final float[][] queryVectors;
    private final int limit;
    @Nullable private final String indexType;
    @Nullable private final RoutedVectorGlobalIndexer routedVectorGlobalIndexer;
    @Nullable private final Set<Integer> explicitRoutedCentroids;

    IvfIndexFileRouter(
            FileStoreTable table,
            @Nullable Predicate filter,
            DataField vectorColumn,
            Map<String, String> options,
            @Nullable float[][] queryVectors,
            int limit,
            @Nullable String indexType,
            @Nullable RoutedVectorGlobalIndexer routedVectorGlobalIndexer) {
        this(
                table,
                filter,
                vectorColumn,
                options,
                queryVectors,
                limit,
                indexType,
                routedVectorGlobalIndexer,
                null);
    }

    IvfIndexFileRouter(
            FileStoreTable table,
            @Nullable Predicate filter,
            DataField vectorColumn,
            Map<String, String> options,
            @Nullable float[][] queryVectors,
            int limit,
            @Nullable String indexType,
            @Nullable RoutedVectorGlobalIndexer routedVectorGlobalIndexer,
            @Nullable Set<Integer> explicitRoutedCentroids) {
        this.table = table;
        this.filter = filter;
        this.vectorColumn = vectorColumn;
        this.options = options;
        this.queryVectors = queryVectors;
        this.limit = limit;
        this.indexType = indexType;
        this.routedVectorGlobalIndexer = routedVectorGlobalIndexer;
        this.explicitRoutedCentroids = explicitRoutedCentroids;
    }

    List<IndexManifestEntry> searchableIndexFileEntries(List<IndexManifestEntry> indexFiles) {
        return indexFiles.stream()
                .filter(f -> f.indexFile().fileKind() != IndexFileKind.ROUTING_MODEL)
                .collect(Collectors.toList());
    }

    List<IndexManifestEntry> routingIndexFileEntries(List<IndexManifestEntry> indexFiles) {
        return indexFiles.stream()
                .filter(f -> f.indexFile().fileKind() == IndexFileKind.ROUTING_MODEL)
                .collect(Collectors.toList());
    }

    @Nullable
    List<IndexFileMeta> tryRouteIndexFiles(
            List<IndexManifestEntry> vectorAndScalarCandidateEntries,
            List<IndexManifestEntry> routingIndexFileEntries) {
        if (!canRoute()) {
            return null;
        }

        RoutedVectorGlobalIndexer routedIndexer = checkNotNull(routedVectorGlobalIndexer);

        Map<BinaryRow, Set<Integer>> routedCentroidsByPartition;
        if (explicitRoutedCentroids != null) {
            routedCentroidsByPartition =
                    explicitRoutedCentroidsByPartition(
                            vectorAndScalarCandidateEntries, explicitRoutedCentroids);
        } else {
            routedCentroidsByPartition =
                    routeCentroidsByPartition(
                            vectorAndScalarCandidateEntries,
                            routingIndexFileEntries,
                            routedIndexer,
                            checkNotNull(queryVectors));
            if (routedCentroidsByPartition == null) {
                return null;
            }
        }

        List<IndexFileMeta> routedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : vectorAndScalarCandidateEntries) {
            GlobalIndexMeta globalIndex = checkNotNull(entry.indexFile().globalIndexMeta());
            if (!isPrimaryColumn(globalIndex, vectorColumn.id())) {
                continue;
            }
            Set<Integer> routedCentroids = routedCentroidsByPartition.get(entry.partition());
            if (routedCentroids == null) {
                continue;
            }
            if (routedIndexer.acceptsRoutedIndexFile(globalIndex.indexMeta(), routedCentroids)) {
                routedIndexFiles.add(entry.indexFile());
            }
        }
        LOG.info(
                "Centroid-based IVF index file routing hit for index type '{}' on vector column '{}', routedCentroidsByPartition={}",
                indexType,
                vectorColumn.name(),
                routedCentroidsByPartitionString(routedCentroidsByPartition));
        return routedIndexFiles;
    }

    List<Set<Integer>> routeCentroids(List<IndexManifestEntry> allIndexFileEntries) {
        return routeCentroids(allIndexFileEntries, null);
    }

    List<Set<Integer>> routeCentroids(
            List<IndexManifestEntry> allIndexFileEntries,
            @Nullable Map<Path, AutoCloseable> routingModelCache) {
        if (!canRoute()) {
            return null;
        }

        RoutedVectorGlobalIndexer routedIndexer = checkNotNull(routedVectorGlobalIndexer);
        float[][] queries = checkNotNull(queryVectors);
        List<IndexManifestEntry> searchableEntries =
                searchableIndexFileEntries(allIndexFileEntries);
        List<IndexManifestEntry> routingEntries = routingIndexFileEntries(allIndexFileEntries);
        Set<BinaryRow> candidateVectorPartitions = candidateVectorPartitions(searchableEntries);
        if (candidateVectorPartitions.isEmpty()) {
            return null;
        }

        Map<BinaryRow, List<GlobalIndexIOMeta>> globalIndexFilesByPartition =
                routingGlobalIndexFilesByPartition(
                        routingEntries,
                        candidateVectorPartitions,
                        table.store().pathFactory().globalIndexFileFactory());
        if (globalIndexFilesByPartition.isEmpty()) {
            return null;
        }

        List<Set<Integer>> routedCentroidsByQuery = new ArrayList<>(queries.length);
        for (int i = 0; i < queries.length; i++) {
            routedCentroidsByQuery.add(new LinkedHashSet<>());
        }

        GlobalIndexFileReader fileReader = fileReader();
        for (BinaryRow partition : candidateVectorPartitions) {
            List<GlobalIndexIOMeta> globalIndexFiles = globalIndexFilesByPartition.get(partition);
            if (globalIndexFiles == null || globalIndexFiles.isEmpty()) {
                return null;
            }
            for (GlobalIndexIOMeta globalIndexFile : globalIndexFiles) {
                List<Set<Integer>> routedCentroidsByVector =
                        routedIndexer.routeCentroidsByVector(
                                fileReader,
                                globalIndexFile,
                                queries,
                                limit,
                                options,
                                routingModelCache);
                if (routedCentroidsByVector.size() != queries.length) {
                    throw new IllegalStateException(
                            "Centroid routing returned "
                                    + routedCentroidsByVector.size()
                                    + " results for "
                                    + queries.length
                                    + " query vectors.");
                }
                for (int i = 0; i < queries.length; i++) {
                    routedCentroidsByQuery.get(i).addAll(routedCentroidsByVector.get(i));
                }
            }
        }
        return routedCentroidsByQuery;
    }

    @Nullable
    private Map<BinaryRow, Set<Integer>> routeCentroidsByPartition(
            List<IndexManifestEntry> vectorAndScalarCandidateEntries,
            List<IndexManifestEntry> routingIndexFileEntries,
            RoutedVectorGlobalIndexer routedIndexer,
            float[][] queries) {
        IndexPathFactory pathFactory = table.store().pathFactory().globalIndexFileFactory();
        GlobalIndexFileReader fileReader = fileReader();
        Set<BinaryRow> candidateVectorPartitions =
                candidateVectorPartitions(vectorAndScalarCandidateEntries);
        if (candidateVectorPartitions.isEmpty()) {
            return null;
        }
        Map<BinaryRow, Set<Integer>> routedCentroidsByPartition = new HashMap<>();
        Map<BinaryRow, List<GlobalIndexIOMeta>> globalIndexFilesByPartition =
                routingGlobalIndexFilesByPartition(
                        routingIndexFileEntries, candidateVectorPartitions, pathFactory);
        if (globalIndexFilesByPartition.isEmpty()) {
            return null;
        }
        for (BinaryRow partition : candidateVectorPartitions) {
            List<GlobalIndexIOMeta> globalIndexFiles = globalIndexFilesByPartition.get(partition);
            if (globalIndexFiles == null || globalIndexFiles.isEmpty()) {
                return null;
            }
            for (GlobalIndexIOMeta globalIndexFile : globalIndexFiles) {
                routedCentroidsByPartition
                        .computeIfAbsent(partition, k -> new HashSet<>())
                        .addAll(
                                routedIndexer.routeCentroids(
                                        fileReader, globalIndexFile, queries, limit, options));
            }
        }
        return routedCentroidsByPartition;
    }

    private GlobalIndexFileReader fileReader() {
        return m -> table.fileIO().newInputStream(m.filePath());
    }

    private Map<BinaryRow, Set<Integer>> explicitRoutedCentroidsByPartition(
            List<IndexManifestEntry> vectorAndScalarCandidateEntries,
            Set<Integer> routedCentroids) {
        Map<BinaryRow, Set<Integer>> routedCentroidsByPartition = new HashMap<>();
        for (BinaryRow partition : candidateVectorPartitions(vectorAndScalarCandidateEntries)) {
            routedCentroidsByPartition.put(partition, routedCentroids);
        }
        return routedCentroidsByPartition;
    }

    private Map<String, String> routedCentroidsByPartitionString(
            Map<BinaryRow, Set<Integer>> routedCentroidsByPartition) {
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<BinaryRow, Set<Integer>> entry : routedCentroidsByPartition.entrySet()) {
            result.put(
                    table.store().pathFactory().getPartitionString(entry.getKey()),
                    entry.getValue().toString());
        }
        return result;
    }

    static boolean canAttemptRouting(
            @Nullable Predicate filter,
            @Nullable float[][] queryVectors,
            int limit,
            @Nullable String indexType) {
        return queryVectors != null
                && queryVectors.length > 0
                && limit > 0
                && filter == null
                && IVF_INDEX_TYPES.contains(indexType);
    }

    private boolean canRoute() {
        return canAttemptRouting(filter, queryVectors, limit, indexType)
                        && routedVectorGlobalIndexer != null
                || explicitRoutedCentroids != null
                        && !explicitRoutedCentroids.isEmpty()
                        && routedVectorGlobalIndexer != null
                        && IVF_INDEX_TYPES.contains(indexType);
    }

    private Set<BinaryRow> candidateVectorPartitions(
            List<IndexManifestEntry> vectorAndScalarCandidateEntries) {
        Set<BinaryRow> result = new HashSet<>();
        for (IndexManifestEntry entry : vectorAndScalarCandidateEntries) {
            IndexFileMeta indexFile = entry.indexFile();
            GlobalIndexMeta globalIndex = checkNotNull(indexFile.globalIndexMeta());
            if (!isPrimaryColumn(globalIndex, vectorColumn.id())
                    || isRoutingGlobalIndexFile(indexFile)) {
                continue;
            }
            result.add(entry.partition());
        }
        return result;
    }

    private Map<BinaryRow, List<GlobalIndexIOMeta>> routingGlobalIndexFilesByPartition(
            List<IndexManifestEntry> routingIndexFileEntries,
            Set<BinaryRow> candidateVectorPartitions,
            IndexPathFactory pathFactory) {
        Map<BinaryRow, List<GlobalIndexIOMeta>> result = new HashMap<>();
        for (IndexManifestEntry entry : routingIndexFileEntries) {
            if (!candidateVectorPartitions.contains(entry.partition())
                    || !isRoutingGlobalIndexFileEntry(entry)) {
                continue;
            }
            IndexFileMeta indexFile = entry.indexFile();
            result.computeIfAbsent(entry.partition(), k -> new ArrayList<>())
                    .add(toGlobalIndexIOMeta(indexFile, pathFactory));
        }
        return result;
    }

    private boolean isRoutingGlobalIndexFileEntry(IndexManifestEntry entry) {
        if (!checkNotNull(indexType).equals(entry.indexFile().indexType())) {
            return false;
        }
        IndexFileMeta indexFile = entry.indexFile();
        GlobalIndexMeta globalIndex = indexFile.globalIndexMeta();
        return globalIndex != null
                && isPrimaryColumn(globalIndex, vectorColumn.id())
                && isRoutingGlobalIndexFile(indexFile);
    }

    private GlobalIndexIOMeta toGlobalIndexIOMeta(
            IndexFileMeta indexFile, IndexPathFactory pathFactory) {
        GlobalIndexMeta globalIndex = checkNotNull(indexFile.globalIndexMeta());
        return new GlobalIndexIOMeta(
                pathFactory.toPath(indexFile),
                indexFile.fileSize(),
                globalIndex.indexMeta(),
                indexFile.fileKind());
    }

    private boolean isRoutingGlobalIndexFile(IndexFileMeta indexFile) {
        if (routedVectorGlobalIndexer == null) {
            return false;
        }
        GlobalIndexMeta globalIndex = indexFile.globalIndexMeta();
        return globalIndex != null
                && routedVectorGlobalIndexer.isRoutingGlobalIndexFile(
                        indexFile.fileKind(), globalIndex.indexMeta());
    }

    private static boolean isPrimaryColumn(GlobalIndexMeta meta, int fieldId) {
        return meta.indexFieldId() == fieldId;
    }
}
