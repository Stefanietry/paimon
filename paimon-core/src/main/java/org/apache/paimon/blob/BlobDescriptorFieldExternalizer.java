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

package org.apache.paimon.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.BlobView;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.blob.BlobFormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileWriterAbortExecutor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Externalizes raw BLOB payloads written to {@code blob-descriptor-field} columns and replaces them
 * with descriptors before normal data-file encoding.
 */
public class BlobDescriptorFieldExternalizer {

    private static final RowType BLOB_ROW_TYPE = RowType.of(DataTypes.BLOB());

    private final FileIO fileIO;
    private final RowDataToObjectArrayConverter rowConverter;
    private final int[] blobFieldIndexes;
    private final ManagedBlobPackWriter packWriter;
    private final Path sidecar;
    private final Set<String> descriptorUris;
    private final List<Path> managedBlobPacks;

    private boolean closed;
    private boolean sidecarCreated;

    public BlobDescriptorFieldExternalizer(
            FileIO fileIO,
            Path dataFile,
            RowType rowType,
            Set<String> blobDescriptorFields,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            int copyBufferSize) {
        this.fileIO = fileIO;
        this.rowConverter = new RowDataToObjectArrayConverter(rowType);
        this.sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        this.descriptorUris = new HashSet<>();
        this.managedBlobPacks = new ArrayList<>();

        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField field = rowType.getFields().get(i);
            if (!blobDescriptorFields.contains(field.name())) {
                continue;
            }
            checkArgument(
                    field.type().getTypeRoot() == DataTypeRoot.BLOB,
                    "BLOB descriptor field '%s' must be BLOB, but was %s.",
                    field.name(),
                    field.type());
            indexes.add(i);
        }
        this.blobFieldIndexes = indexes.stream().mapToInt(Integer::intValue).toArray();
        this.packWriter =
                new ManagedBlobPackWriter(
                        fileIO, pathFactory, targetFileSize, managedBlobPacks, copyBufferSize);
    }

    public boolean enabled() {
        return blobFieldIndexes.length > 0;
    }

    public InternalRow externalize(InternalRow row) throws IOException {
        if (!enabled()) {
            return row;
        }

        GenericRow result = null;
        try {
            for (int fieldIndex : blobFieldIndexes) {
                if (row.isNullAt(fieldIndex)) {
                    continue;
                }

                Blob blob = row.getBlob(fieldIndex);
                if (blob instanceof BlobRef) {
                    collect(((BlobRef) blob).toDescriptor());
                    continue;
                }
                if (blob instanceof BlobView) {
                    continue;
                }

                if (result == null) {
                    result = copy(row);
                }
                BlobDescriptor descriptor = packWriter.write(blob);
                collect(descriptor);
                result.setField(
                        fieldIndex,
                        Blob.fromFile(
                                fileIO, descriptor.uri(), descriptor.offset(), descriptor.length()));
            }
        } catch (IOException | RuntimeException e) {
            abort();
            throw e;
        }
        return result == null ? row : result;
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            packWriter.closeCurrent();
            if (!descriptorUris.isEmpty()) {
                List<ManagedBlobReferenceFile.Reference> references =
                        new ArrayList<>(descriptorUris.size());
                for (String descriptorUri : descriptorUris) {
                    ManagedBlobReferenceFile.fromDescriptorUri(descriptorUri)
                            .ifPresent(references::add);
                }
                if (!references.isEmpty()) {
                    ManagedBlobReferenceFile.write(fileIO, sidecar, references);
                    sidecarCreated = true;
                }
            }
            closed = true;
        } catch (IOException e) {
            abort();
            throw e;
        }
    }

    public void abort() {
        packWriter.abortCurrent();
        fileIO.deleteQuietly(sidecar);
        sidecarCreated = false;
        for (Path path : managedBlobPacks) {
            fileIO.deleteQuietly(path);
        }
        closed = true;
    }

    @Nullable
    public String result() {
        return sidecarCreated ? sidecar.getName() : null;
    }

    public FileWriterAbortExecutor abortExecutor() {
        return new FileWriterAbortExecutor(fileIO, sidecar) {
            @Override
            public void abort() {
                fileIO.deleteQuietly(sidecar);
                for (Path path : managedBlobPacks) {
                    fileIO.deleteQuietly(path);
                }
            }
        };
    }

    private void collect(BlobDescriptor descriptor) {
        descriptorUris.add(descriptor.uri());
    }

    private GenericRow copy(InternalRow row) {
        GenericRow copied = rowConverter.toGenericRow(row);
        copied.setRowKind(row.getRowKind());
        return copied;
    }

    private static class ManagedBlobPackWriter {

        private final FileIO fileIO;
        private final DataFilePathFactory pathFactory;
        private final long targetFileSize;
        private final List<Path> managedBlobPacks;
        private final int copyBufferSize;

        private Path currentPath;
        private PositionOutputStream out;
        private BlobFormatWriter writer;
        private BlobDescriptor lastDescriptor;

        private ManagedBlobPackWriter(
                FileIO fileIO,
                DataFilePathFactory pathFactory,
                long targetFileSize,
                List<Path> managedBlobPacks,
                int copyBufferSize) {
            this.fileIO = fileIO;
            this.pathFactory = pathFactory;
            this.targetFileSize = targetFileSize;
            this.managedBlobPacks = managedBlobPacks;
            this.copyBufferSize = copyBufferSize;
        }

        private BlobDescriptor write(Blob blob) throws IOException {
            if (writer == null) {
                openCurrent();
            }

            lastDescriptor = null;
            writer.addElement(GenericRow.ofKind(RowKind.INSERT, blob));
            BlobDescriptor descriptor = lastDescriptor;
            if (descriptor == null) {
                throw new IOException("Managed BLOB writer did not produce a descriptor.");
            }
            if (writer.reachTargetSize(true, targetFileSize)) {
                closeCurrent();
            }
            return descriptor;
        }

        private void openCurrent() throws IOException {
            currentPath =
                    pathFactory.newPathFromExtension(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
            managedBlobPacks.add(currentPath);
            out = fileIO.newOutputStream(currentPath, false);
            writer =
                    new BlobFormatWriter(
                            out,
                            (fieldName, descriptor) -> {
                                lastDescriptor = descriptor;
                                return false;
                            },
                            BLOB_ROW_TYPE,
                            false,
                            false,
                            BlobFetchMetricReporter.NOOP,
                            copyBufferSize);
            writer.setFile(currentPath);
        }

        private void closeCurrent() throws IOException {
            if (writer == null) {
                return;
            }
            PositionOutputStream currentOut = out;
            try (PositionOutputStream ignored = currentOut) {
                writer.close();
                currentOut.flush();
            } finally {
                writer = null;
                out = null;
                currentPath = null;
            }
        }

        private void abortCurrent() {
            IOUtils.closeQuietly(writer);
            IOUtils.closeQuietly(out);
            writer = null;
            out = null;
            currentPath = null;
        }
    }
}
