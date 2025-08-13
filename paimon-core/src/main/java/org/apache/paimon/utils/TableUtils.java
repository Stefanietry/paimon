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
import org.apache.paimon.chain.ChainQueryType;
import org.apache.paimon.chain.ChainSinkType;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;

import java.util.Map;

/** Utils for table. */
public class TableUtils {

    public static boolean isChainTbl(Map<String, String> tableOptions) {
        return Boolean.parseBoolean(tableOptions.get(CoreOptions.CHAIN_TABLE_ENABLED.key()));
    }

    public static boolean isChainBranch(Map<String, String> tableOptions) {
        String currentBranch =
                tableOptions.getOrDefault(
                        CoreOptions.BRANCH.key(), CoreOptions.BRANCH.defaultValue());
        return isChainTbl(tableOptions)
                && (currentBranch.equalsIgnoreCase(
                                tableOptions.get(CoreOptions.CHAIN_TABLE_SNAPSHOT_BRANCH.key()))
                        || currentBranch.equalsIgnoreCase(
                                tableOptions.get(CoreOptions.CHAIN_TABLE_DELTA_BRANCH.key())));
    }

    public static boolean isChainRead(Map<String, String> tableOptions) {
        return isChainBranch(tableOptions)
                && ChainQueryType.CHAIN_READ
                        .getValue()
                        .equalsIgnoreCase(
                                tableOptions.getOrDefault(
                                        CoreOptions.CHAIN_TABLE_QUERY_TYPE.key(),
                                        CoreOptions.CHAIN_TABLE_QUERY_TYPE.defaultValue()));
    }

    public static FileStoreTable getWriteTable(FileStoreTable fileStoreTable) {
        if (TableUtils.isChainTbl(fileStoreTable.options())) {
            String sinkType = fileStoreTable.options().get(CoreOptions.CHAIN_TABLE_SINK_TYPE.key());
            String sinkBranch = ChainSinkType.getSinkBranchName(sinkType, fileStoreTable.options());
            FileStoreTable candidateFileStoreTable =
                    fileStoreTable instanceof FallbackReadFileStoreTable
                            ? ((FallbackReadFileStoreTable) fileStoreTable).primaryTable()
                            : fileStoreTable;
            assert candidateFileStoreTable instanceof PrimaryKeyFileStoreTable;
            if (sinkBranch.equalsIgnoreCase(
                    ((PrimaryKeyFileStoreTable) candidateFileStoreTable).currentBranch())) {
                return fileStoreTable;
            }
            return candidateFileStoreTable.switchToBranch(sinkBranch);
        } else {
            return fileStoreTable;
        }
    }
}
