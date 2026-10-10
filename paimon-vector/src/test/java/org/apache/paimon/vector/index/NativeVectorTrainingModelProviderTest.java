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

package org.apache.paimon.vector.index;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.VectorIndexTrainer;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NativeVectorTrainingModelProvider}. */
public class NativeVectorTrainingModelProviderTest {

    @Test
    public void testReusableTrainingModelRoundTrip() throws Exception {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        Map<String, String> options = ivfPqOptions(2);
        float[] vectors = trainingVectors(512);
        NativeVectorTrainingModelProvider provider = new NativeVectorTrainingModelProvider();
        try (VectorGlobalModelTrainer trainer = provider.createTrainer("ivf-pq", options)) {
            for (int i = 0; i < vectors.length / 2; i++) {
                trainer.write(new float[] {vectors[i * 2], vectors[i * 2 + 1]}, i);
            }
            try (IvfTrainingModel trained = (IvfTrainingModel) trainer.finishTraining()) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                trained.serializeTo(output);

                try (IvfTrainingModel restored =
                        (IvfTrainingModel)
                                provider.loadModel("ivf-pq", options, output.toByteArray())) {
                    assertThat(restored.routingModel().nlist()).isEqualTo(2);
                    int centroid =
                            restored.routingModel().assignCentroid(new float[] {-1.0f, 0.0f});
                    assertThat(centroid).isBetween(0, 1);
                    assertThat(
                                    restored.routingModel()
                                            .nearestCentroids(new float[] {-1.0f, 0.0f}, 1))
                            .containsExactly(centroid);
                }
            }
        }
    }

    @Test
    public void testTrainingModelCreatesMultipleShardWritersWithoutBeingConsumed()
            throws Exception {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        Map<String, String> options = ivfPqOptions(2);
        float[] vectors = trainingVectors(512);
        NativeVectorTrainingModelProvider provider = new NativeVectorTrainingModelProvider();
        try (VectorGlobalModelTrainer trainer = provider.createTrainer("ivf-pq", options)) {
            for (int i = 0; i < vectors.length / 2; i++) {
                trainer.write(new float[] {vectors[i * 2], vectors[i * 2 + 1]}, i);
            }
            try (IvfTrainingModel model = (IvfTrainingModel) trainer.finishTraining()) {
                int firstCentroid = model.routingModel().assignCentroid(new float[] {-1.0f, 0.0f});
                int secondCentroid = model.routingModel().assignCentroid(new float[] {1.0f, 0.0f});
                assertThat(firstCentroid).isNotEqualTo(secondCentroid);

                InMemoryFileWriter firstFiles = new InMemoryFileWriter();
                GlobalIndexSingleColumnWriter firstWriter =
                        model.createCentroidShardIndexWriter(firstFiles, firstCentroid);
                try {
                    firstWriter.write(new float[] {-1.0f, 0.0f}, 10L);
                    assertThat(firstWriter.finish()).hasSize(1);
                } finally {
                    ((AutoCloseable) firstWriter).close();
                }

                InMemoryFileWriter secondFiles = new InMemoryFileWriter();
                GlobalIndexSingleColumnWriter secondWriter =
                        model.createCentroidShardIndexWriter(secondFiles, secondCentroid);
                try {
                    secondWriter.write(new float[] {1.0f, 0.0f}, 20L);
                    assertThat(secondWriter.finish()).hasSize(1);
                } finally {
                    ((AutoCloseable) secondWriter).close();
                }

                // Each shard consumes a restored training copy. The original remains usable.
                assertThat(model.routingModel().nlist()).isEqualTo(2);
                assertThat(firstFiles.bytes()).isNotEmpty();
                assertThat(secondFiles.bytes()).isNotEmpty();
            }
        }
    }

    @Test
    public void testCentroidShardFileNameIsUriSafe() throws Exception {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        Map<String, String> options = ivfPqOptions(2);
        float[] vectors = trainingVectors(512);
        NativeVectorTrainingModelProvider provider = new NativeVectorTrainingModelProvider();
        try (VectorGlobalModelTrainer trainer = provider.createTrainer("ivf-pq", options)) {
            for (int i = 0; i < vectors.length / 2; i++) {
                trainer.write(new float[] {vectors[i * 2], vectors[i * 2 + 1]}, i);
            }
            try (IvfTrainingModel model = (IvfTrainingModel) trainer.finishTraining()) {
                int centroid = model.routingModel().assignCentroid(new float[] {-1.0f, 0.0f});
                for (String root :
                        new String[] {"file:///tmp/global-index", "hdfs://namenode/global-index"}) {
                    PathValidatingInMemoryFileWriter files =
                            new PathValidatingInMemoryFileWriter(new Path(root));
                    GlobalIndexSingleColumnWriter writer =
                            model.createCentroidShardIndexWriter(files, centroid);
                    try {
                        writer.write(new float[] {-1.0f, 0.0f}, 10L);
                        List<ResultEntry> results = writer.finish();
                        assertThat(results).hasSize(1);
                        assertThat(results.get(0).fileName()).startsWith("vector-centroid-");
                        assertThat(files.path().toUri().getScheme())
                                .isEqualTo(new Path(root).toUri().getScheme());
                    } finally {
                        ((AutoCloseable) writer).close();
                    }
                }
            }
        }
    }

    private static boolean isNativeAvailable() {
        try (VectorIndexTrainer ignored = VectorIndexTrainer.create(ivfPqOptions(1))) {
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

    private static Map<String, String> ivfPqOptions(int nlist) {
        Map<String, String> options = new HashMap<>();
        options.put("index.type", "ivf_pq");
        options.put("dimension", "2");
        options.put("metric", "l2");
        options.put("nlist", Integer.toString(nlist));
        options.put("pq.m", "1");
        return options;
    }

    private static float[] trainingVectors(int count) {
        float[] vectors = new float[count * 2];
        for (int i = 0; i < count; i++) {
            vectors[i * 2] = i < count / 2 ? -1.0f : 1.0f;
            vectors[i * 2 + 1] = (i % 17) * 0.01f;
        }
        return vectors;
    }

    private static class InMemoryFileWriter implements GlobalIndexFileWriter {

        private final ByteArrayPositionOutputStream output = new ByteArrayPositionOutputStream();

        @Override
        public String newFileName(String prefix) {
            return prefix + ".index";
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) {
            return output;
        }

        byte[] bytes() {
            return output.toByteArray();
        }
    }

    private static class PathValidatingInMemoryFileWriter extends InMemoryFileWriter {

        private final Path directory;
        private Path path;

        private PathValidatingInMemoryFileWriter(Path directory) {
            this.directory = directory;
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) {
            path = new Path(directory, fileName);
            return super.newOutputStream(fileName);
        }

        private Path path() {
            return path;
        }
    }

    private static class ByteArrayPositionOutputStream extends PositionOutputStream {

        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        @Override
        public long getPos() {
            return output.size();
        }

        @Override
        public void write(int value) {
            output.write(value);
        }

        @Override
        public void write(byte[] bytes) throws IOException {
            output.write(bytes);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            output.write(bytes, offset, length);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}

        byte[] toByteArray() {
            return output.toByteArray();
        }
    }
}
