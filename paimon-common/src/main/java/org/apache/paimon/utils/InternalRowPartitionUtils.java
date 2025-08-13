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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** PartitionComputer for {@link InternalRow}. */
public class InternalRowPartitionUtils {

    public static final String DEFAULT_PARTITION_PATH_EQUALS_STR = "=";
    public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

    public static Map<BinaryRow, BinaryRow> findFirstLatestPartitions(
            List<BinaryRow> sortedSourcePartitions,
            List<BinaryRow> sortedTargetPartitions,
            RecordComparator partitionComparator) {
        Map<BinaryRow, BinaryRow> partitionMapping = new HashMap<>();
        int targetIndex = sortedTargetPartitions.size() - 1;
        for (BinaryRow sourceRow : sortedSourcePartitions) {
            BinaryRow firstSmaller = null;
            while (targetIndex >= 0) {
                BinaryRow targetRow = sortedTargetPartitions.get(targetIndex);
                if (partitionComparator.compare(targetRow, sourceRow) < 0) {
                    firstSmaller = targetRow;
                    break;
                }
                targetIndex--;
            }
            partitionMapping.put(sourceRow, firstSmaller);
        }
        return partitionMapping;
    }

    public static List<BinaryRow> getDeltaPartitions(
            BinaryRow beginPartition,
            BinaryRow endPartition,
            List<String> partitionColumns,
            RowType partType,
            CoreOptions options,
            RecordComparator partitionComparator,
            InternalRowPartitionComputer partitionComputer) {
        InternalRowSerializer serializer = new InternalRowSerializer(partType);
        List<BinaryRow> deltaPartitions = new ArrayList<>();
        boolean isDailyPartition = partitionColumns.size() == 1;
        List<String> startPartitionValues =
                partitionComputer.generateOrderPartValues(beginPartition);
        List<String> endPartitionValues = partitionComputer.generateOrderPartValues(endPartition);
        String startDate = startPartitionValues.get(0);
        String endDate = endPartitionValues.get(0);
        String candidateDate = startDate;
        while (candidateDate.compareTo(endDate) <= 0) {
            if (isDailyPartition) {
                if (candidateDate.compareTo(startDate) > 0) {
                    deltaPartitions.add(
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    ListUtils.convertListsToMap(
                                                            partitionColumns,
                                                            Arrays.asList(candidateDate)),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy());
                }
            } else {
                for (int hour = 0; hour <= 23; hour++) {
                    List<String> candidatePartitionValues =
                            Arrays.asList(
                                    candidateDate,
                                    String.format(options.partitionDatePattern(), hour));
                    BinaryRow candidatePartition =
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    ListUtils.convertListsToMap(
                                                            partitionColumns,
                                                            candidatePartitionValues),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy();
                    if (partitionComparator.compare(candidatePartition, beginPartition) > 0
                            && partitionComparator.compare(candidatePartition, endPartition) <= 0) {
                        deltaPartitions.add(candidatePartition);
                    }
                }
            }
            candidateDate = DateTimeUtils.getNextDay(candidateDate, options.partitionDatePattern());
        }
        return deltaPartitions;
    }

    public static String toHivePartitionString(
            List<String> partitionColumns, List<String> partitionValues) {
        if (partitionColumns.size() != partitionValues.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Column names and values must have the "
                                    + "same size. Got columns %s and values %s",
                            partitionColumns, partitionValues));
        }
        return IntStream.range(0, partitionColumns.size())
                .mapToObj(
                        i -> {
                            String column = partitionColumns.get(i);
                            String value = partitionValues.get(i);
                            return String.format(
                                    "%s".concat(DEFAULT_PARTITION_PATH_EQUALS_STR).concat("%s"),
                                    column,
                                    value);
                        })
                .collect(Collectors.joining(DEFAULT_PARTITION_PATH_SEPARATOR));
    }
}
