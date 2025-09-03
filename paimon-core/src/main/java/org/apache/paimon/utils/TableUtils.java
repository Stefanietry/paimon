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

import org.apache.paimon.ChainQueryType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.ChainFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Map;

/** Utils for table. */
public class TableUtils {

    public static boolean isChainTbl(Map<String, String> tableOptions) {
        return Boolean.parseBoolean(tableOptions.get(CoreOptions.CHAIN_TABLE_ENABLED.key()));
    }

    public static boolean isChainFallbackReadSnapshotBranch(Map<String, String> tableOptions) {
        String currentBranch =
                tableOptions.getOrDefault(
                        CoreOptions.BRANCH.key(), CoreOptions.BRANCH.defaultValue());
        return isChainTbl(tableOptions)
                && currentBranch.equalsIgnoreCase(
                        tableOptions.get(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key()));
    }

    public static boolean isChainFallbackReadDeltaBranch(Map<String, String> tableOptions) {
        String currentBranch =
                tableOptions.getOrDefault(
                        CoreOptions.BRANCH.key(), CoreOptions.BRANCH.defaultValue());
        return isChainTbl(tableOptions)
                && currentBranch.equalsIgnoreCase(
                        tableOptions.get(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key()));
    }

    public static boolean isChainFallbackReadSnapshotBranch(
            Map<String, String> tableOptions, String currentBranch) {
        return isChainTbl(tableOptions)
                && currentBranch.equalsIgnoreCase(
                        tableOptions.get(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key()));
    }

    public static boolean isChainFallbackReadDeltaBranch(
            Map<String, String> tableOptions, String currentBranch) {
        return isChainTbl(tableOptions)
                && currentBranch.equalsIgnoreCase(
                        tableOptions.get(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key()));
    }

    public static boolean isChainFallbackReadBranch(Map<String, String> tableOptions) {
        return isChainFallbackReadSnapshotBranch(tableOptions)
                || isChainFallbackReadDeltaBranch(tableOptions);
    }

    public static boolean isChainFallbackReadBranch(
            Map<String, String> tableOptions, String currentBranch) {
        return isChainFallbackReadSnapshotBranch(tableOptions, currentBranch)
                || isChainFallbackReadDeltaBranch(tableOptions, currentBranch);
    }

    public static boolean isChainBranchInternalReadMode(Map<String, String> tableOptions) {
        return isChainFallbackReadBranch(tableOptions)
                && ChainQueryType.CHAIN_READ
                        .getValue()
                        .equalsIgnoreCase(
                                tableOptions.getOrDefault(
                                        CoreOptions.CHAIN_TABLE_BRANCH_INTERNAL_USAGE_MODE.key(),
                                        CoreOptions.CHAIN_TABLE_BRANCH_INTERNAL_USAGE_MODE
                                                .defaultValue()));
    }

    public static FileStoreTable getWriteTable(FileStoreTable fileStoreTable) {
        if (TableUtils.isChainTbl(fileStoreTable.options())) {
            FileStoreTable candidateFileStoreTable =
                    fileStoreTable instanceof FallbackReadFileStoreTable
                            ? ((FallbackReadFileStoreTable) fileStoreTable).primaryTable()
                            : fileStoreTable;
            Preconditions.checkArgument(
                    candidateFileStoreTable instanceof PrimaryKeyFileStoreTable,
                    "Chain table must be primary key table.");
            return candidateFileStoreTable;
        } else {
            return fileStoreTable;
        }
    }

    public static Table getReadTable(Table table) {
        if (isChainTbl(table.options()) && isChainFallbackReadBranch(table.options())) {
            if (table instanceof FallbackReadFileStoreTable) {
                if (isChainFallbackReadSnapshotBranch(table.options())) {
                    return ((ChainFileStoreTable) (((FallbackReadFileStoreTable) table).fallback()))
                            .primaryTable();
                } else {
                    return ((ChainFileStoreTable) (((FallbackReadFileStoreTable) table).fallback()))
                            .fallback();
                }
            }
        }
        return table;
    }
}
