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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

/** Write result meta. */
public class ResultEntry {

    private final String fileName;

    /** Total logical rows processed by the writer, including null/skipped rows. */
    private final long rowCount;

    @Nullable private final byte[] meta;

    @Nullable private final IndexFileKind fileKind;

    @Nullable private final Range rowRange;

    public ResultEntry(String fileName, long rowCount, @Nullable byte[] meta) {
        this(fileName, rowCount, meta, null, null);
    }

    public ResultEntry(
            String fileName,
            long rowCount,
            @Nullable byte[] meta,
            @Nullable IndexFileKind fileKind,
            @Nullable Range rowRange) {
        this.fileName = fileName;
        this.rowCount = rowCount;
        this.meta = meta;
        this.fileKind = fileKind;
        this.rowRange = rowRange;
    }

    public String fileName() {
        return fileName;
    }

    public long rowCount() {
        return rowCount;
    }

    public byte[] meta() {
        return meta;
    }

    @Nullable
    public IndexFileKind fileKind() {
        return fileKind;
    }

    @Nullable
    public Range rowRange() {
        return rowRange;
    }
}
