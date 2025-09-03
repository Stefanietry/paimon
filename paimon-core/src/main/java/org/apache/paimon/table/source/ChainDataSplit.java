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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** A specific implementation for {@link DataSplit} on chain table. */
public class ChainDataSplit extends DataSplit {

    public static final int VIRTUAL_SNAPSHOT = -1;
    public static final String VIRTUAL_BUCKET_PATH = "placeholder::virtual-bucket-path";
    private BinaryRow readPartition;

    private HashMap<String, String> fileBucketPathMapping;

    public ChainDataSplit(
            BinaryRow readPartition,
            int bucket,
            List<DataSplit> splits,
            HashMap<String, String> fileBucketPathMapping) {
        this.readPartition = readPartition;
        this.fileBucketPathMapping = fileBucketPathMapping;
        DataSplit split =
                splits.size() > 1 ? mergeSplits(readPartition, bucket, splits) : splits.get(0);
        assign(split);
    }

    public ChainDataSplit(
            DataSplit split,
            BinaryRow readPartition,
            HashMap<String, String> fileBucketPathMapping) {
        this.readPartition = readPartition;
        this.fileBucketPathMapping = fileBucketPathMapping;
        assign(split);
    }

    public DataSplit mergeSplits(BinaryRow readPartition, int bucket, List<DataSplit> splits) {
        List<Integer> totalBucketSet =
                splits.stream()
                        .map(split -> split.totalBuckets())
                        .distinct()
                        .collect(Collectors.toList());
        if (totalBucketSet.size() != 1) {
            throw new IllegalStateException(
                    String.format(
                            "totalBuckets must be same, " + "but got %s, bucket %s",
                            totalBucketSet, bucket));
        }
        List<DataFileMeta> beforeFiles =
                splits.stream()
                        .map(split -> split.beforeFiles())
                        .filter(
                                subBeforeFiles ->
                                        subBeforeFiles != null && !subBeforeFiles.isEmpty())
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        List<DeletionFile> beforeDeletionFiles =
                splits.stream().allMatch(split -> !split.beforeDeletionFiles().isPresent())
                        ? null
                        : splits.stream()
                                .map(split -> split.beforeDeletionFiles())
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .filter(
                                        subBeforeDeletionFiles ->
                                                subBeforeDeletionFiles != null
                                                        && !subBeforeDeletionFiles.isEmpty())
                                .flatMap(List::stream)
                                .collect(Collectors.toList());
        List<DataFileMeta> dataFiles =
                splits.stream()
                        .map(split -> split.dataFiles())
                        .filter(subDataFiles -> subDataFiles != null && !subDataFiles.isEmpty())
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        List<DeletionFile> dataDeletionFiles =
                splits.stream().allMatch(split -> split.dataDeletionFiles() == null)
                        ? null
                        : splits.stream()
                                .map(split -> split.dataDeletionFiles())
                                .filter(
                                        subDataDeletionFiles ->
                                                subDataDeletionFiles != null
                                                        && !subDataDeletionFiles.isEmpty())
                                .flatMap(List::stream)
                                .collect(Collectors.toList());
        boolean isStreaming = splits.stream().allMatch(split -> (split.isStreaming()));
        boolean rawConvertible =
                splits.size() == 1 && splits.stream().allMatch(split -> (split.rawConvertible()));
        DataSplit.Builder dataSplitBuilder =
                DataSplit.builder()
                        .withSnapshot(VIRTUAL_SNAPSHOT)
                        .withPartition(readPartition)
                        .withBucket(bucket)
                        .withBucketPath(VIRTUAL_BUCKET_PATH)
                        .withTotalBuckets(totalBucketSet.get(0))
                        .withBeforeFiles(beforeFiles)
                        .withDataFiles(dataFiles)
                        .isStreaming(isStreaming)
                        .rawConvertible(rawConvertible);
        if (beforeDeletionFiles != null) {
            dataSplitBuilder.withBeforeDeletionFiles(beforeDeletionFiles);
        }
        if (dataDeletionFiles != null) {
            dataSplitBuilder.withDataDeletionFiles(dataDeletionFiles);
        }
        return dataSplitBuilder.build();
    }

    @Override
    public BinaryRow readPartition() {
        return readPartition;
    }

    @Override
    public HashMap<String, String> fileBucketPathMapping() {
        return fileBucketPathMapping;
    }

    @Override
    public boolean equals(Object o) {
        boolean isSame = super.equals(o);
        if (isSame) {
            DataSplit chainDataSplit = (DataSplit) o;
            return Objects.equals(readPartition, chainDataSplit.readPartition())
                    && fileBucketPathMapping.equals(chainDataSplit.fileBucketPathMapping());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), readPartition, fileBucketPathMapping);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        ChainDataSplit split = deserialize(new DataInputViewStreamWrapper(in));
        this.readPartition = split.readPartition();
        this.fileBucketPathMapping = split.fileBucketPathMapping();
        assign(split);
    }

    @Override
    public void serialize(DataOutputView out) throws IOException {
        super.serialize(out);
        SerializationUtils.serializeBinaryRow(readPartition, out);
        out.writeInt(fileBucketPathMapping.size());
        for (Map.Entry<String, String> entry : fileBucketPathMapping.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    public static ChainDataSplit deserialize(DataInputView in) throws IOException {
        DataSplit dataSplit = DataSplit.deserialize(in);
        BinaryRow readPartition = SerializationUtils.deserializeBinaryRow(in);
        int size = in.readInt();
        HashMap<String, String> fileBucketPathMapping = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            fileBucketPathMapping.put(key, value);
        }
        return new ChainDataSplit(dataSplit, readPartition, fileBucketPathMapping);
    }

    @Override
    public String dataSplitType() {
        return DataSplitType.CHAIN_DATA_SPLIT.name();
    }
}
