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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.IvfShard;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;
import org.apache.paimon.vector.index.IvfTrainingModel;
import org.apache.paimon.vector.index.VectorGlobalModelTrainer;
import org.apache.paimon.vector.index.VectorTrainingModel;
import org.apache.paimon.vector.index.VectorTrainingModelProviderRegistry;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import scala.Tuple2;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_BUILD_CENTROID_ASSIGN_BATCH_SIZE;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_TRAIN_SAMPLE_ROWS;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.CENTROID_TRAIN_MODE_LOCAL;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.CENTROID_TRAIN_MODE_OPTION;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.GLOBAL_INDEX_FILE_EXTENSION;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.IVF_SHARD_OPTION;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.VECTOR_ROUTING_MODEL_FILE_PREFIX;

/** Topology builder for centroid-sharded IVF global indexes. */
class CentroidShardedIvfIndexTopoBuilder {

    private static final Logger LOG =
            LoggerFactory.getLogger(CentroidShardedIvfIndexTopoBuilder.class);

    List<CommitMessage> buildIndex(
            SparkSession spark,
            PartitionPredicate partitionPredicate,
            FileStoreTable table,
            String indexType,
            RowType readType,
            DataField indexField,
            List<DataField> extraFields,
            Options options)
            throws IOException {
        checkArgument(
                extraFields.isEmpty(),
                "Option '%s=%s' currently supports only one vector column.",
                IVF_SHARD_OPTION,
                IvfShard.CENTROID_BASED.optionValue());
        checkArgument(
                CENTROID_TRAIN_MODE_LOCAL.equals(
                        options.getString(CENTROID_TRAIN_MODE_OPTION, CENTROID_TRAIN_MODE_LOCAL)),
                "The centroid-sharded implementation of '%s=%s' currently supports only '%s=%s'.",
                IVF_SHARD_OPTION,
                IvfShard.CENTROID_BASED.optionValue(),
                CENTROID_TRAIN_MODE_OPTION,
                CENTROID_TRAIN_MODE_LOCAL);

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return Collections.emptyList();
        }

        CentroidShardedIvfIndexBuildPlanner planner =
                CentroidShardedIvfIndexBuildPlanner.create(
                        table, snapshot, indexType, indexField, partitionPredicate);
        if (planner.isEmpty()) {
            return Collections.emptyList();
        }

        List<DataField> indexFields = planner.indexFields();
        List<Range> rowRangesToBuild = planner.rowRangesToBuild();
        List<IndexedSplit> splits = planner.splits();

        Map<String, String> nativeOptions =
                CentroidShardedIvfIndexBuilder.nativeOptions(indexType, indexField, options);
        long trainingSampleRows = options.get(GLOBAL_INDEX_TRAIN_SAMPLE_ROWS);
        checkArgument(
                trainingSampleRows > 0,
                "Option '%s' must be greater than 0.",
                GLOBAL_INDEX_TRAIN_SAMPLE_ROWS.key());
        int centroidAssignBatchSize = options.get(GLOBAL_INDEX_BUILD_CENTROID_ASSIGN_BATCH_SIZE);
        checkArgument(
                centroidAssignBatchSize > 0,
                "Option '%s' must be greater than 0.",
                GLOBAL_INDEX_BUILD_CENTROID_ASSIGN_BATCH_SIZE.key());
        List<IndexedSplit> trainingSplits =
                limitSplitsToTrainingSampleRows(splits, trainingSampleRows);
        IvfTrainingModel trainingModel;
        try (VectorGlobalModelTrainer trainer =
                VectorTrainingModelProviderRegistry.createTrainer(indexType, nativeOptions)) {
            collectTrainingSamples(table, readType, indexField, trainingSplits, trainer);
            trainingModel = asIvfTrainingModel(trainer.finishTraining());
        }

        List<ResultEntry> resultEntries;
        try {
            resultEntries =
                    buildDataShards(
                            spark,
                            table,
                            readType,
                            indexField,
                            splits,
                            trainingModel,
                            indexType,
                            nativeOptions,
                            centroidAssignBatchSize,
                            options);
            if (resultEntries.isEmpty()) {
                return Collections.emptyList();
            }
        } finally {
            trainingModel.close();
        }

        Range indexRange = CentroidShardedIvfIndexBuilder.coveringRange(rowRangesToBuild);
        List<IndexFileMeta> indexFileMetas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        indexRange,
                        indexFields,
                        indexType,
                        resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return Collections.singletonList(
                new CommitMessageImpl(
                        splits.get(0).dataSplit().partition(),
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement()));
    }

    private static void collectTrainingSamples(
            FileStoreTable table,
            RowType readType,
            DataField indexField,
            List<IndexedSplit> splits,
            VectorGlobalModelTrainer trainer)
            throws IOException {
        for (IndexedSplit split : splits) {
            CentroidShardedIvfIndexBuilder indexBuilder =
                    CentroidShardedIvfIndexBuilder.forTrainingSamples(
                            table, readType, indexField, split.rowRanges());
            ReadBuilder builder = table.newReadBuilder();
            builder.withReadType(readType);
            try (RecordReader<InternalRow> recordReader = builder.newRead().createReader(split);
                    CloseableIterator<InternalRow> data = recordReader.toCloseableIterator()) {
                indexBuilder.writeTrainingSamples(data, trainer);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Failed to collect centroid training samples.", e);
            }
        }
    }

    static List<IndexedSplit> limitSplitsToTrainingSampleRows(
            List<IndexedSplit> splits, long trainingSampleRows) {
        if (trainingSampleRows < 0) {
            return splits;
        }

        List<IndexedSplit> result = new ArrayList<>();
        long selectedRows = 0;
        for (IndexedSplit split : splits) {
            List<DataFileMeta> selectedFiles = new ArrayList<>();
            for (DataFileMeta file : split.dataSplit().dataFiles()) {
                selectedFiles.add(file);
                selectedRows = saturatedAdd(selectedRows, file.rowCount());
                if (selectedRows > trainingSampleRows) {
                    break;
                }
            }
            if (!selectedFiles.isEmpty()) {
                result.add(copySplitWithDataFiles(split, selectedFiles));
            }
            if (selectedRows > trainingSampleRows) {
                break;
            }
        }
        return result;
    }

    private static IndexedSplit copySplitWithDataFiles(
            IndexedSplit split, List<DataFileMeta> dataFiles) {
        DataSplit dataSplit = split.dataSplit();
        DataSplit sampledDataSplit =
                DataSplit.builder()
                        .withPartition(dataSplit.partition())
                        .withBucket(dataSplit.bucket())
                        .withTotalBuckets(dataSplit.totalBuckets())
                        .withDataFiles(dataFiles)
                        .withBucketPath(dataSplit.bucketPath())
                        .rawConvertible(dataSplit.rawConvertible())
                        .build();
        return new IndexedSplit(sampledDataSplit, split.rowRanges(), split.scores());
    }

    private static long saturatedAdd(long left, long right) {
        long result = left + right;
        if (((left ^ result) & (right ^ result)) < 0) {
            return Long.MAX_VALUE;
        }
        return result;
    }

    private static List<ResultEntry> buildDataShards(
            SparkSession spark,
            FileStoreTable table,
            RowType readType,
            DataField indexField,
            List<IndexedSplit> splits,
            IvfTrainingModel trainingModel,
            String indexType,
            Map<String, String> nativeOptions,
            int centroidAssignBatchSize,
            Options options)
            throws IOException {
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        byte[] trainingModelPayload = serializeTrainingModelPayload(trainingModel);
        int nlist = trainingModel.routingModel().nlist();
        if (splits.isEmpty()) {
            return Collections.emptyList();
        }

        CentroidShardedIvfIndexBuilder assignBuilder =
                CentroidShardedIvfIndexBuilder.forCentroidAssignment(
                        table,
                        readType,
                        indexField,
                        Collections.emptyList(),
                        indexType,
                        nativeOptions,
                        trainingModelPayload,
                        centroidAssignBatchSize);
        Broadcast<byte[]> assignBuilderBytes =
                javaSparkContext.broadcast(InstantiationUtil.serializeObject(assignBuilder));

        int scanParallelism = DefaultGlobalIndexTopoBuilder.parallelism(splits.size(), options);
        LOG.info(
                "Building centroid-sharded IVF data shards with split size {} and scan parallelism {}.",
                splits.size(),
                scanParallelism);
        List<byte[]> splitPayloads = serializeIndexedSplitPayloads(splits);
        JavaPairRDD<Integer, CentroidShardedIvfIndexBuilder.AssignedVector> assignedVectors =
                javaSparkContext
                        .parallelize(splitPayloads, scanParallelism)
                        .mapPartitionsToPair(
                                splitPayloadsInPartition ->
                                        assignCentroidVectorsPartition(
                                                assignBuilderBytes, splitPayloadsInPartition));

        CentroidShardedIvfIndexBuilder shardBuilder =
                CentroidShardedIvfIndexBuilder.forShardBuild(
                        table, indexType, nativeOptions, trainingModelPayload);
        Broadcast<byte[]> shardBuilderBytes =
                javaSparkContext.broadcast(InstantiationUtil.serializeObject(shardBuilder));
        List<byte[]> shardResultBytes;
        try {
            shardResultBytes =
                    assignedVectors
                            .partitionBy(new CentroidPartitioner(nlist))
                            .mapPartitions(
                                    partition -> buildShardPartition(partition, shardBuilderBytes))
                            .collect();
        } finally {
            assignBuilderBytes.destroy(false);
            shardBuilderBytes.destroy(false);
        }

        List<CentroidShardedIvfIndexBuilder.ShardBuildResult> dataShardResults = new ArrayList<>();
        ClassLoader classLoader = CentroidShardedIvfIndexTopoBuilder.class.getClassLoader();
        for (byte[] resultBytes : shardResultBytes) {
            try {
                CentroidShardedIvfIndexBuilder.ShardBuildResult result =
                        InstantiationUtil.deserializeObject(resultBytes, classLoader);
                dataShardResults.add(result);
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize centroid shard build result.", e);
            }
        }

        if (dataShardResults.isEmpty()) {
            return Collections.emptyList();
        }

        List<ResultEntry> resultEntries = new ArrayList<>();
        for (CentroidShardedIvfIndexBuilder.ShardBuildResult dataShardResult : dataShardResults) {
            resultEntries.add(dataShardResult.toResultEntry());
        }

        String routingModelFileName = newRoutingModelFileName();
        CentroidShardedIvfIndexBuilder globalIndexFileBuilder =
                CentroidShardedIvfIndexBuilder.forGlobalIndexFile(
                        table, trainingModel, indexType, nativeOptions);
        resultEntries.add(
                globalIndexFileBuilder.writeVectorGlobalIndexFile(routingModelFileName, indexType));
        return resultEntries;
    }

    private static String newRoutingModelFileName() {
        return VECTOR_ROUTING_MODEL_FILE_PREFIX
                + '-'
                + UUID.randomUUID()
                + GLOBAL_INDEX_FILE_EXTENSION;
    }

    private static byte[] serializeTrainingModelPayload(VectorTrainingModel trainingModel)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        trainingModel.serializeTo(out);
        return out.toByteArray();
    }

    private static List<byte[]> serializeIndexedSplitPayloads(List<IndexedSplit> splits)
            throws IOException {
        List<byte[]> splitPayloads = new ArrayList<>(splits.size());
        for (IndexedSplit split : splits) {
            splitPayloads.add(serializeIndexedSplitPayload(split));
        }
        return splitPayloads;
    }

    static byte[] serializeIndexedSplitPayload(IndexedSplit split) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(out));
        return out.toByteArray();
    }

    static IndexedSplit deserializeIndexedSplitPayload(byte[] splitPayload) throws IOException {
        return IndexedSplit.deserialize(
                new DataInputViewStreamWrapper(new ByteArrayInputStream(splitPayload)));
    }

    private static IvfTrainingModel asIvfTrainingModel(VectorTrainingModel trainingModel) {
        if (!(trainingModel instanceof IvfTrainingModel)) {
            throw new IllegalStateException(
                    "Centroid-sharded IVF index requires IvfTrainingModel, but got: "
                            + trainingModel.getClass().getName());
        }
        return (IvfTrainingModel) trainingModel;
    }

    private static Iterator<Tuple2<Integer, CentroidShardedIvfIndexBuilder.AssignedVector>>
            assignCentroidVectorsPartition(
                    Broadcast<byte[]> indexBuilderBytes, Iterator<byte[]> splitPayloads)
                    throws Exception {
        if (!splitPayloads.hasNext()) {
            return Collections.emptyIterator();
        }

        ClassLoader classLoader = CentroidShardedIvfIndexTopoBuilder.class.getClassLoader();
        CentroidShardedIvfIndexBuilder indexBuilder =
                InstantiationUtil.deserializeObject(indexBuilderBytes.value(), classLoader);
        ReadBuilder builder = indexBuilder.table().newReadBuilder();
        builder.withReadType(indexBuilder.readType());
        return new CentroidAssignmentPartitionIterator(indexBuilder, builder, splitPayloads);
    }

    private static Iterator<Tuple2<Integer, CentroidShardedIvfIndexBuilder.AssignedVector>>
            assignCentroidVectors(
                    CentroidShardedIvfIndexBuilder indexBuilder,
                    ReadBuilder builder,
                    IndexedSplit split)
                    throws Exception {
        RecordReader<InternalRow> recordReader = null;
        CloseableIterator<InternalRow> data = null;
        try {
            recordReader = builder.newRead().createReader(split);
            data = recordReader.toCloseableIterator();
            return new ClosingIterator<>(
                    indexBuilder.assignCentroidVectorsLazy(data, split.rowRanges()),
                    data,
                    recordReader);
        } catch (Throwable t) {
            closeQuietly(data, t);
            closeQuietly(recordReader, t);
            throw t;
        }
    }

    private static Iterator<byte[]> buildShardPartition(
            Iterator<Tuple2<Integer, CentroidShardedIvfIndexBuilder.AssignedVector>> partition,
            Broadcast<byte[]> shardBuilderBytes)
            throws Exception {
        if (!partition.hasNext()) {
            return Collections.emptyIterator();
        }

        ClassLoader classLoader = CentroidShardedIvfIndexTopoBuilder.class.getClassLoader();
        CentroidShardedIvfIndexBuilder indexBuilder =
                InstantiationUtil.deserializeObject(shardBuilderBytes.value(), classLoader);
        try {
            List<byte[]> resultEntries = new ArrayList<>();
            for (CentroidShardedIvfIndexBuilder.ShardBuildResult result :
                    indexBuilder.buildCentroidShards(partition)) {
                resultEntries.add(InstantiationUtil.serializeObject(result));
            }
            return resultEntries.iterator();
        } finally {
            indexBuilder.close();
        }
    }

    private static void closeQuietly(AutoCloseable closeable, Throwable error) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Throwable closeError) {
            error.addSuppressed(closeError);
        }
    }

    /** Partitions assigned vectors directly by centroid. */
    private static class CentroidPartitioner extends Partitioner {

        private static final long serialVersionUID = 1L;

        private final int nlist;

        private CentroidPartitioner(int nlist) {
            checkArgument(nlist > 0, "Centroid partitioner requires positive nlist.");
            this.nlist = nlist;
        }

        @Override
        public int numPartitions() {
            return nlist;
        }

        @Override
        public int getPartition(Object key) {
            checkArgument(
                    key instanceof Integer,
                    "Centroid partitioner requires Integer key, but was %s.",
                    key == null ? "null" : key.getClass().getName());
            int centroid = (Integer) key;
            checkArgument(
                    centroid >= 0 && centroid < nlist,
                    "Centroid %s is out of range [0, %s).",
                    centroid,
                    nlist);
            return centroid;
        }
    }

    private static class CentroidAssignmentPartitionIterator
            implements Iterator<Tuple2<Integer, CentroidShardedIvfIndexBuilder.AssignedVector>> {

        private final CentroidShardedIvfIndexBuilder indexBuilder;
        private final ReadBuilder builder;
        private final Iterator<byte[]> splitPayloads;
        private Iterator<Tuple2<Integer, CentroidShardedIvfIndexBuilder.AssignedVector>>
                assignedVectors = Collections.emptyIterator();
        private boolean closed;

        private CentroidAssignmentPartitionIterator(
                CentroidShardedIvfIndexBuilder indexBuilder,
                ReadBuilder builder,
                Iterator<byte[]> splitPayloads) {
            this.indexBuilder = indexBuilder;
            this.builder = builder;
            this.splitPayloads = splitPayloads;
        }

        @Override
        public boolean hasNext() {
            try {
                while (!assignedVectors.hasNext()) {
                    if (!splitPayloads.hasNext()) {
                        close();
                        return false;
                    }

                    IndexedSplit split = deserializeIndexedSplitPayload(splitPayloads.next());
                    assignedVectors = assignCentroidVectors(indexBuilder, builder, split);
                }
                return true;
            } catch (RuntimeException | Error e) {
                closeQuietly(e);
                throw e;
            } catch (Exception e) {
                RuntimeException error =
                        new RuntimeException("Failed to assign vectors to centroids.", e);
                closeQuietly(error);
                throw error;
            }
        }

        @Override
        public Tuple2<Integer, CentroidShardedIvfIndexBuilder.AssignedVector> next() {
            try {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return assignedVectors.next();
            } catch (RuntimeException | Error e) {
                closeQuietly(e);
                throw e;
            }
        }

        private void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                indexBuilder.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close centroid assignment builder.", e);
            }
        }

        private void closeQuietly(Throwable error) {
            if (closed) {
                return;
            }
            closed = true;
            CentroidShardedIvfIndexTopoBuilder.closeQuietly(indexBuilder, error);
        }
    }

    private static class ClosingIterator<T> implements Iterator<T> {

        private final Iterator<T> delegate;
        private final AutoCloseable[] closeables;
        private boolean closed;

        private ClosingIterator(Iterator<T> delegate, AutoCloseable... closeables) {
            this.delegate = delegate;
            this.closeables = closeables;
        }

        @Override
        public boolean hasNext() {
            try {
                boolean hasNext = delegate.hasNext();
                if (!hasNext) {
                    close();
                }
                return hasNext;
            } catch (RuntimeException | Error e) {
                closeQuietly(e);
                throw e;
            }
        }

        @Override
        public T next() {
            try {
                return delegate.next();
            } catch (RuntimeException | Error e) {
                closeQuietly(e);
                throw e;
            }
        }

        private void close() {
            if (closed) {
                return;
            }
            closed = true;
            RuntimeException error = null;
            for (AutoCloseable closeable : closeables) {
                if (closeable == null) {
                    continue;
                }
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (error == null) {
                        error =
                                new RuntimeException(
                                        "Failed to close centroid assignment iterator.", e);
                    } else {
                        error.addSuppressed(e);
                    }
                }
            }
            if (error != null) {
                throw error;
            }
        }

        private void closeQuietly(Throwable error) {
            if (closed) {
                return;
            }
            closed = true;
            for (AutoCloseable closeable : closeables) {
                if (closeable == null) {
                    continue;
                }
                try {
                    closeable.close();
                } catch (Throwable closeError) {
                    error.addSuppressed(closeError);
                }
            }
        }
    }
}
