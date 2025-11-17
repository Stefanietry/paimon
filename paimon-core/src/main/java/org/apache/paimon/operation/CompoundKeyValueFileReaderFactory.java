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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.ReadContext;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FormatReaderMapping;

import static org.apache.paimon.CoreOptions.BRANCH;
import static org.apache.paimon.CoreOptions.SCAN_FALLBACK_DELTA_BRANCH;
import static org.apache.paimon.CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH;

/** A specific implementation about {@link KeyValueFileReaderFactory} for chain table. */
public class CompoundKeyValueFileReaderFactory extends KeyValueFileReaderFactory {

    private final SchemaManager snapshotSchemaManager;

    private final SchemaManager deltaSchemaManager;

    private final String snapshotBranch;

    private final String deltaBranch;

    private final String currentBranch;

    private final ReadContext readContext;

    public CompoundKeyValueFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            FormatReaderMapping.Builder formatReaderMappingBuilder,
            DataFilePathFactory pathFactory,
            long asyncThreshold,
            BinaryRow partition,
            DeletionVector.Factory dvFactory,
            ReadContext readContext) {
        super(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                formatReaderMappingBuilder,
                pathFactory,
                asyncThreshold,
                partition,
                dvFactory);
        this.readContext = readContext;
        this.currentBranch = super.schema().options().get(BRANCH.key());
        this.snapshotBranch = super.schema().options().get(SCAN_FALLBACK_SNAPSHOT_BRANCH.key());
        this.deltaBranch = super.schema().options().get(SCAN_FALLBACK_DELTA_BRANCH.key());
        this.snapshotSchemaManager =
                snapshotBranch.equalsIgnoreCase(currentBranch)
                        ? super.schemaManager()
                        : super.schemaManager().copyWithBranch(snapshotBranch);
        this.deltaSchemaManager =
                deltaBranch.equalsIgnoreCase(currentBranch)
                        ? super.schemaManager()
                        : super.schemaManager().copyWithBranch(deltaBranch);
    }

    public SchemaManager getSchemaManager(String fileName) {
        if (snapshotBranch.equalsIgnoreCase(readContext.fileBranchMapping().get(fileName))) {
            return snapshotSchemaManager;
        } else {
            return deltaSchemaManager;
        }
    }

    @Override
    protected TableSchema getDataSchema(DataFileMeta fileMeta) {
        if (currentBranch.equalsIgnoreCase(
                readContext.fileBranchMapping().get(fileMeta.fileName()))) {
            return super.getDataSchema(fileMeta);
        }
        return getSchemaManager(fileMeta.fileName()).schema(fileMeta.schemaId());
    }

    @Override
    protected BinaryRow getReadPartition() {
        return readContext.readPartition();
    }
}
