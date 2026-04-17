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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.BundleRecords;

import java.io.IOException;

public class VortexStagingWriter implements BundleFormatWriter {
    private final BundleFormatWriter delegate;
    private final FileIO targetFileIO;
    private final Path targetPath;
    private final java.nio.file.Path localFile;

    public VortexStagingWriter(
            BundleFormatWriter delegate,
            FileIO targetFileIO,
            Path targetPath,
            java.nio.file.Path localFile) {
        this.delegate = delegate;
        this.targetFileIO = targetFileIO;
        this.targetPath = targetPath;
        this.localFile = localFile;
    }

    @Override
    public void addElement(InternalRow internalRow) throws IOException {
        delegate.addElement(internalRow);
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        delegate.writeBundle(bundleRecords);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return delegate.reachTargetSize(suggestedCheck, targetSize);
    }

    @Override
    public void close() throws IOException {
        IOException error = null;
        try {
            delegate.close();
            VortexUtils.uploadLocalFile(targetFileIO, localFile, targetPath);
        } catch (IOException e) {
            error = e;
        } finally {
            VortexUtils.deleteLocalFileQuietly(localFile);
        }

        if (error != null) {
            throw error;
        }
    }
}
