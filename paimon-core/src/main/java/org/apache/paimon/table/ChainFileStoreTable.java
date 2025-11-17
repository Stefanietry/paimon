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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.ReusableDataTableBatchScan;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.Preconditions;

import java.util.Optional;

public class ChainFileStoreTable extends PrimaryKeyFileStoreTable {

    private static final long serialVersionUID = 1L;

    public ChainFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }

    @Override
    public DataTableBatchScan newScan() {
        if (ChainTableUtils.isScanFallbackChainRead(coreOptions())) {
            return new ReusableDataTableBatchScan(
                    tableSchema,
                    schemaManager(),
                    coreOptions(),
                    newSnapshotReader(),
                    catalogEnvironment.tableQueryAuth(coreOptions()));
        }
        return super.newScan();
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        String currentBranch = BranchManager.normalizeBranch(currentBranch());
        String targetBranch = BranchManager.normalizeBranch(branchName);
        if (currentBranch.equals(targetBranch)) {
            return this;
        }
        Optional<TableSchema> optionalSchema =
                new SchemaManager(fileIO(), location(), targetBranch).latest();
        Preconditions.checkArgument(
                optionalSchema.isPresent(), "Branch " + targetBranch + " does not exist");

        TableSchema branchSchema = optionalSchema.get();
        Options branchOptions = new Options(branchSchema.options());
        branchOptions.set(CoreOptions.BRANCH, targetBranch);
        branchSchema = branchSchema.copy(branchOptions.toMap());
        Identifier currentIdentifier = identifier();
        CatalogEnvironment newCatalogEnvironment =
                catalogEnvironment.copy(
                        new Identifier(
                                currentIdentifier.getDatabaseName(),
                                currentIdentifier.getTableName(),
                                targetBranch,
                                currentIdentifier.getSystemTableName()));
        return FileStoreTableFactory.create(
                fileIO(), location(), branchSchema, new Options(), newCatalogEnvironment);
    }
}
