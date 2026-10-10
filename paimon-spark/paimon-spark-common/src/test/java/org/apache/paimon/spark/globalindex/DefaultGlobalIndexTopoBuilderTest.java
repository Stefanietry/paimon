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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.globalindex.sorted.SortedIndexTopoBuilder;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;
import org.apache.paimon.vector.index.VectorGlobalModelTrainer;
import org.apache.paimon.vector.index.VectorTrainingModel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_BUILD_MAX_PARALLELISM;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultGlobalIndexTopoBuilder}. */
public class DefaultGlobalIndexTopoBuilderTest {

    @Test
    void testBitmapUsesSortedTopologyBuilder() {
        assertThat(GlobalIndexTopologyBuilderUtils.createTopoBuilder("bitmap"))
                .isInstanceOf(SortedIndexTopoBuilder.class);
    }

    @Test
    void testFMIndexUsesDefaultTopologyBuilder() {
        assertThat(GlobalIndexTopologyBuilderUtils.createTopoBuilder("fm"))
                .isInstanceOf(DefaultGlobalIndexTopoBuilder.class);
    }

    @Test
    void testRowsPerShardUsesMergedBuildOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.key(), "1000");
        Map<String, String> buildOptions = new HashMap<>();
        buildOptions.put(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.key(), "25");

        assertThat(
                        DefaultGlobalIndexTopoBuilder.rowsPerShard(
                                new Options(tableOptions, buildOptions)))
                .isEqualTo(25L);
    }

    @Test
    void testParallelismUsesBuildMaxParallelism() {
        Options options =
                new Options(
                        Collections.singletonMap(GLOBAL_INDEX_BUILD_MAX_PARALLELISM.key(), "2"));

        assertThat(DefaultGlobalIndexTopoBuilder.parallelism(5, options)).isEqualTo(2);
        assertThat(DefaultGlobalIndexTopoBuilder.parallelism(1, options)).isEqualTo(1);
    }

    @Test
    void testRowsPerShardMustBePositive() {
        Options options =
                new Options(Collections.singletonMap(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.key(), "0"));

        assertThatThrownBy(() -> DefaultGlobalIndexTopoBuilder.rowsPerShard(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Option 'global-index.row-count-per-shard' must be greater than 0.");
    }

    @Test
    void testMaxParallelismMustBePositive() {
        Options options =
                new Options(
                        Collections.singletonMap(GLOBAL_INDEX_BUILD_MAX_PARALLELISM.key(), "0"));

        assertThatThrownBy(() -> DefaultGlobalIndexTopoBuilder.parallelism(5, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Option 'global-index.build.max-parallelism' must be greater than 0.");
    }

    @Test
    void testLimitSplitsToTrainingSampleRowsSelectsFilesByFileMetaRowCount() {
        IndexedSplit split =
                indexedSplit(
                        "bucket-0",
                        dataFile("file-0", 12, 0),
                        dataFile("file-1", 12, 12),
                        dataFile("file-2", 12, 24),
                        dataFile("file-3", 12, 36));

        List<IndexedSplit> sampledSplits =
                CentroidShardedIvfIndexTopoBuilder.limitSplitsToTrainingSampleRows(
                        Collections.singletonList(split), 30);

        assertThat(sampledSplits).hasSize(1);
        assertThat(fileNames(sampledSplits.get(0))).containsExactly("file-0", "file-1", "file-2");
    }

    @Test
    void testCentroidNativeOptionsKeepIvfOptions() {
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put("ivf-pq.expected-vector-count", "1000");
        optionMap.put("ivf-pq.pq.code-ratio", "8");
        optionMap.put("fields.vec.pq.use-opq", "true");
        Options options = new Options(optionMap);

        Map<String, String> nativeOptions =
                CentroidShardedIvfIndexBuilder.nativeOptions(
                        "ivf-pq",
                        new DataField(0, "vec", new VectorType(2, DataTypes.FLOAT())),
                        options);

        assertThat(nativeOptions)
                .containsEntry("expected-vector-count", "1000")
                .containsEntry("pq.code-ratio", "8")
                .containsEntry("use-opq", "true")
                .containsEntry("dimension", "2");
    }

    @Test
    void testCentroidTrainingSamplesSupportVectorType() {
        RowType readType =
                RowType.of(
                        SpecialFields.ROW_ID,
                        new DataField(0, "vec", new VectorType(2, DataTypes.FLOAT())));
        CentroidShardedIvfIndexBuilder builder =
                CentroidShardedIvfIndexBuilder.forTrainingSamples(
                        null,
                        readType,
                        new DataField(0, "vec", new VectorType(2, DataTypes.FLOAT())),
                        Collections.singletonList(new Range(0, 10)));

        List<CentroidShardedIvfIndexBuilder.TrainingSample> samples =
                builder.collectTrainingSamples(
                        CloseableIterator.adapterForIterator(
                                Collections.<InternalRow>singletonList(
                                                GenericRow.of(
                                                        7L,
                                                        BinaryVector.fromPrimitiveArray(
                                                                new float[] {1.0f, 2.0f})))
                                        .iterator()));

        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).absoluteRowId()).isEqualTo(7L);
        assertThat(samples.get(0).vector()).containsExactly(1.0f, 2.0f);
    }

    @Test
    void testCentroidTrainingSamplesCanBeWrittenStreaming() throws Exception {
        RowType readType =
                RowType.of(
                        SpecialFields.ROW_ID,
                        new DataField(0, "vec", new VectorType(2, DataTypes.FLOAT())));
        CentroidShardedIvfIndexBuilder builder =
                CentroidShardedIvfIndexBuilder.forTrainingSamples(
                        null,
                        readType,
                        new DataField(0, "vec", new VectorType(2, DataTypes.FLOAT())),
                        Collections.singletonList(new Range(0, 10)));
        RecordingTrainer trainer = new RecordingTrainer();

        builder.writeTrainingSamples(
                CloseableIterator.adapterForIterator(
                        Arrays.<InternalRow>asList(
                                        GenericRow.of(
                                                7L,
                                                BinaryVector.fromPrimitiveArray(
                                                        new float[] {1.0f, 2.0f})),
                                        GenericRow.of(
                                                11L,
                                                BinaryVector.fromPrimitiveArray(
                                                        new float[] {3.0f, 4.0f})))
                                .iterator()),
                trainer);

        assertThat(trainer.rowIds).containsExactly(7L);
        assertThat(trainer.vectors).hasSize(1);
        assertThat(trainer.vectors.get(0)).containsExactly(1.0f, 2.0f);
    }

    @Test
    void testCentroidPartitionRowRangesUseFilteredDataFiles() {
        List<ManifestEntry> entries =
                Arrays.asList(
                        ManifestEntry.create(
                                FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, dataFile("file-1", 4, 8)),
                        ManifestEntry.create(
                                FileKind.ADD,
                                BinaryRow.EMPTY_ROW,
                                0,
                                1,
                                dataFile("file-2", 4, 12)));

        assertThat(CentroidShardedIvfIndexBuildPlanner.partitionDataFileRowRanges(entries))
                .containsExactly(new Range(8, 15));
    }

    @Test
    void testCentroidPartitionIndexedSplitsUseOneDataFilePerSplit() {
        List<ManifestEntry> entries =
                Arrays.asList(
                        ManifestEntry.create(
                                FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, dataFile("file-1", 4, 8)),
                        ManifestEntry.create(
                                FileKind.ADD,
                                BinaryRow.EMPTY_ROW,
                                0,
                                1,
                                dataFile("file-2", 4, 12)));

        List<IndexedSplit> splits =
                CentroidShardedIvfIndexBuildPlanner.createPartitionIndexedSplits(
                        entries,
                        Collections.singletonList(new Range(8, 15)),
                        (partition, bucket) -> "bucket-0");

        assertThat(splits).hasSize(2);
        assertThat(fileNames(splits.get(0))).containsExactly("file-1");
        assertThat(fileNames(splits.get(1))).containsExactly("file-2");
        assertThat(splits.get(0).rowRanges()).containsExactly(new Range(8, 15));
        assertThat(splits.get(1).rowRanges()).containsExactly(new Range(8, 15));
    }

    @Test
    void testCentroidPartitionIndexedSplitsKeepDataEvolutionFileGroup() {
        List<ManifestEntry> entries =
                Arrays.asList(
                        ManifestEntry.create(
                                FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, dataFile("file-1", 4, 8)),
                        ManifestEntry.create(
                                FileKind.ADD,
                                BinaryRow.EMPTY_ROW,
                                0,
                                1,
                                dataFile("file-1.vector.lance", 4, 8)));

        List<IndexedSplit> splits =
                CentroidShardedIvfIndexBuildPlanner.createPartitionIndexedSplits(
                        entries,
                        Collections.singletonList(new Range(8, 11)),
                        (partition, bucket) -> "bucket-0");

        assertThat(splits).hasSize(1);
        assertThat(fileNames(splits.get(0))).containsExactly("file-1", "file-1.vector.lance");
        assertThat(splits.get(0).rowRanges()).containsExactly(new Range(8, 11));
    }

    @Test
    void testCentroidSplitPayloadKeepsPartitionSegments() throws Exception {
        BinaryRow partition = binaryIntRow(7);
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(3)
                        .withTotalBuckets(4)
                        .withDataFiles(Collections.singletonList(dataFile("file-1", 4, 8)))
                        .withBucketPath("pt=7/bucket-3")
                        .rawConvertible(false)
                        .build();
        IndexedSplit split =
                new IndexedSplit(dataSplit, Collections.singletonList(new Range(8, 11)), null);

        IndexedSplit restored =
                CentroidShardedIvfIndexTopoBuilder.deserializeIndexedSplitPayload(
                        CentroidShardedIvfIndexTopoBuilder.serializeIndexedSplitPayload(split));

        assertThat(restored.dataSplit().partition().getInt(0)).isEqualTo(7);
        assertThat(restored.dataSplit().bucket()).isEqualTo(3);
        assertThat(restored.rowRanges()).containsExactly(new Range(8, 11));
    }

    private static IndexedSplit indexedSplit(String bucketPath, DataFileMeta... dataFiles) {
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withTotalBuckets(1)
                        .withDataFiles(Arrays.asList(dataFiles))
                        .withBucketPath(bucketPath)
                        .rawConvertible(false)
                        .build();
        return new IndexedSplit(dataSplit, Collections.singletonList(new Range(0, 100)), null);
    }

    private static BinaryRow binaryIntRow(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }

    private static DataFileMeta dataFile(String fileName, long rowCount, long firstRowId) {
        return DataFileMeta.forAppend(
                fileName,
                0,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                firstRowId,
                null);
    }

    private static List<String> fileNames(IndexedSplit split) {
        return split.dataSplit().dataFiles().stream()
                .map(DataFileMeta::fileName)
                .collect(Collectors.toList());
    }

    private static class RecordingTrainer implements VectorGlobalModelTrainer {

        private final List<Long> rowIds = new ArrayList<>();
        private final List<float[]> vectors = new ArrayList<>();

        @Override
        public void write(Object vector, long absoluteRowId) {
            rowIds.add(absoluteRowId);
            vectors.add(((BinaryVector) vector).toFloatArray());
        }

        @Override
        public VectorTrainingModel finishTraining() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
