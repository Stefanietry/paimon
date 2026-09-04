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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.VectorIndexTrainer;
import org.apache.paimon.index.vector.VectorIndexTraining;
import org.apache.paimon.index.vector.VectorIndexWriter;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.vector.index.NativeVectorIndexOptions.IVF_FLAT_INDEX_TYPE;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.IVF_PQ_INDEX_TYPE;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.IVF_RQ_INDEX_TYPE;
import static org.apache.paimon.vector.index.NativeVectorIndexOptions.IVF_SQ_INDEX_TYPE;

/** Native provider for paimon-vector-index training models. */
public class NativeVectorTrainingModelProvider implements VectorTrainingModelProvider {

    private static final int TRAIN_BATCH_SIZE = 4096;
    private static final Set<String> SUPPORTED_INDEX_TYPES =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(
                            Arrays.asList(
                                    IVF_PQ_INDEX_TYPE,
                                    IVF_FLAT_INDEX_TYPE,
                                    IVF_SQ_INDEX_TYPE,
                                    IVF_RQ_INDEX_TYPE)));

    public NativeVectorTrainingModelProvider() {}

    @Override
    public String identifier() {
        return IVF_PQ_INDEX_TYPE;
    }

    @Override
    public Set<String> supportedIndexTypes() {
        return SUPPORTED_INDEX_TYPES;
    }

    @Override
    public VectorGlobalModelTrainer createTrainer(String indexType, Map<String, String> options) {
        ensureSupported(indexType);
        return new NativeVectorGlobalModelTrainer(indexType, options);
    }

    @Override
    public VectorTrainingModel loadModel(
            String indexType, Map<String, String> options, byte[] payload) throws IOException {
        ensureSupported(indexType);
        return NativeVectorTrainingModel.load(
                indexType, options, new ByteArrayInputStream(payload));
    }

    private static void ensureSupported(String indexType) {
        if (!SUPPORTED_INDEX_TYPES.contains(indexType)) {
            throw new IllegalArgumentException(
                    "Unsupported native vector training model index type: " + indexType);
        }
    }

    /**
     * Java-side trainer for the centroid global IVF model.
     *
     * <p>This class is intentionally not a placeholder. It owns the same Java-side work as {@link
     * NativeVectorGlobalIndexWriter}: validate/materialize vectors, batch them, feed the existing
     * native {@link VectorIndexTrainer}, and convert the native training result into a Paimon
     * {@link VectorTrainingModel}. The native dependencies after {@link
     * VectorIndexTrainer#finishTraining()} are intentionally declared as methods in this class, and
     * currently fail fast until paimon-vector-index provides the corresponding APIs and
     * implementations on {@link VectorIndexTraining} and {@link VectorIndexWriter}.
     */
    private static class NativeVectorGlobalModelTrainer implements VectorGlobalModelTrainer {

        private final String indexType;
        private final Map<String, String> options;
        private final int dim;
        private final VectorIndexTrainer trainer;
        private final float[] vectorBuf;
        private final int trainBatchSize;
        private float[] batchVectors;
        private int batchCount;
        private long count;
        private boolean finished;

        private NativeVectorGlobalModelTrainer(String indexType, Map<String, String> options) {
            this.indexType = indexType;
            this.options = new LinkedHashMap<>(options);
            this.dim = parseDimension(options);
            this.trainer = VectorIndexTrainer.create(options);
            this.vectorBuf = new float[dim];
            this.trainBatchSize =
                    NativeVectorGlobalIndexWriter.vectorBatchSize(TRAIN_BATCH_SIZE, dim);
            this.batchVectors = new float[trainBatchSize * dim];
        }

        @Override
        public void write(@Nullable Object key, long absoluteRowId) {
            ensureNotFinished();
            if (key == null) {
                return;
            }
            float[] vector = materializeAndValidate(key, absoluteRowId);
            System.arraycopy(vector, 0, batchVectors, batchCount * dim, dim);
            batchCount++;
            count++;
            if (batchCount == trainBatchSize) {
                try {
                    flushTrainingBatch();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to flush native vector training batch.", e);
                }
            }
        }

        @Override
        public VectorTrainingModel finishTraining() throws IOException {
            ensureNotFinished();
            finished = true;
            if (count == 0) {
                close();
                throw new IllegalStateException(
                        "Native global model trainer for '"
                                + indexType
                                + "' requires at least one non-null training vector.");
            }

            flushTrainingBatch();
            VectorIndexTraining training = trainer.finishTraining();
            closeTrainerQuietly();
            batchVectors = null;
            return new NativeVectorTrainingModel(indexType, options, training);
        }

        @Override
        public void close() throws IOException {
            batchVectors = null;
            closeTrainerQuietly();
        }

        private void flushTrainingBatch() throws IOException {
            if (batchCount == 0) {
                return;
            }
            if (batchCount == trainBatchSize) {
                trainer.addTrainingVectors(batchVectors, batchCount);
            } else {
                trainer.addTrainingVectors(
                        Arrays.copyOf(batchVectors, batchCount * dim), batchCount);
            }
            batchCount = 0;
        }

        private float[] materializeAndValidate(Object fieldData, long absoluteRowId) {
            if (fieldData instanceof float[]) {
                float[] vector = (float[]) fieldData;
                checkDimension(vector.length);
                for (int i = 0; i < dim; i++) {
                    checkFinite(vector[i], absoluteRowId, i);
                }
                return vector;
            } else if (fieldData instanceof InternalVector) {
                InternalVector vector = (InternalVector) fieldData;
                checkDimension(vector.size());
                for (int i = 0; i < dim; i++) {
                    float v = vector.getFloat(i);
                    checkFinite(v, absoluteRowId, i);
                    vectorBuf[i] = v;
                }
                return vectorBuf;
            } else if (fieldData instanceof InternalArray) {
                InternalArray array = (InternalArray) fieldData;
                checkDimension(array.size());
                for (int i = 0; i < dim; i++) {
                    if (array.isNullAt(i)) {
                        throw new IllegalArgumentException(
                                "Vector element at index " + i + " is null");
                    }
                    float v = array.getFloat(i);
                    checkFinite(v, absoluteRowId, i);
                    vectorBuf[i] = v;
                }
                return vectorBuf;
            }
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }

        private void checkDimension(int actualDim) {
            if (actualDim != dim) {
                throw new IllegalArgumentException(
                        String.format(
                                "Vector dimension mismatch: expected %d, but got %d",
                                dim, actualDim));
            }
        }

        private void checkFinite(float value, long absoluteRowId, int elementIndex) {
            if (!Float.isFinite(value)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Vector element at rowId=%d, index=%d is %s",
                                absoluteRowId, elementIndex, Float.toString(value)));
            }
        }

        private void ensureNotFinished() {
            if (finished) {
                throw new IllegalStateException(
                        "Native vector global model training has finished.");
            }
        }

        private void closeTrainerQuietly() {
            trainer.close();
        }
    }

    private static int parseDimension(Map<String, String> options) {
        String dimension = options.get("dimension");
        if (dimension == null) {
            throw new IllegalArgumentException(
                    "Native vector training requires option 'dimension'.");
        }
        int dim = Integer.parseInt(dimension);
        if (dim <= 0) {
            throw new IllegalArgumentException(
                    "Native vector training requires positive dimension.");
        }
        return dim;
    }

    static class NativeVectorTrainingModel implements IvfTrainingModel {

        private String indexType;
        private Map<String, String> options;
        private transient VectorIndexTraining training;
        private transient byte[] serializedTraining;

        private NativeVectorTrainingModel(
                String indexType, Map<String, String> options, VectorIndexTraining training) {
            this.indexType = indexType;
            this.options = new LinkedHashMap<>(options);
            this.training = training;
        }

        static NativeVectorTrainingModel load(
                String indexType, Map<String, String> options, InputStream payload)
                throws IOException {
            byte[] serializedTraining = readFully(payload);
            NativeVectorTrainingModel model =
                    new NativeVectorTrainingModel(
                            indexType,
                            options,
                            deserializeVectorIndexTraining(serializedTraining, options));
            model.serializedTraining = serializedTraining;
            return model;
        }

        @Override
        public IvfRoutingModel routingModel() {
            return new NativeIvfRoutingModel(options, training());
        }

        @Override
        public GlobalIndexSingleColumnWriter createCentroidShardIndexWriter(
                GlobalIndexFileWriter fileWriter, int centroid) {
            return new NativeIvfCentroidShardIndexWriter(fileWriter, this, centroid, options);
        }

        @Override
        public void serializeTo(OutputStream out) throws IOException {
            out.write(serializedTraining());
        }

        VectorIndexWriter createCentroidShardWriter(int centroid) {
            return createVectorIndexWriter(training(), centroid, options);
        }

        void addCentroidVectors(
                VectorIndexWriter writer, long[] ids, float[] vectors, int vectorCount) {
            NativeVectorTrainingModelProvider.addCentroidVectors(
                    indexType, writer, ids, vectors, vectorCount, options);
        }

        @Override
        public void close() throws IOException {
            if (training != null) {
                training.close();
                training = null;
            }
        }

        private byte[] serializedTraining() throws IOException {
            if (serializedTraining == null) {
                serializedTraining = serializeVectorIndexTraining(training(), options);
            }
            return serializedTraining;
        }

        private VectorIndexTraining training() {
            if (training == null) {
                throw new IllegalStateException("Native vector training model has been closed.");
            }
            return training;
        }
    }

    private static class NativeIvfRoutingModel implements IvfRoutingModel {

        private final Map<String, String> options;
        private final VectorIndexTraining training;

        private NativeIvfRoutingModel(Map<String, String> options, VectorIndexTraining training) {
            this.options = new LinkedHashMap<>(options);
            this.training = training;
        }

        @Override
        public int nlist() {
            return parsePositiveOption(options, "nlist");
        }

        @Override
        public int assignCentroid(float[] vector) {
            return NativeVectorTrainingModelProvider.assignCentroid(training, vector, options);
        }

        @Override
        public int[] nearestCentroids(float[] queryVector, int nprobe) {
            return findNearestCentroids(training, queryVector, nprobe, options);
        }

        private static int parsePositiveOption(Map<String, String> options, String key) {
            String value = options.get(key);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Native vector training requires option '" + key + "'.");
            }
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(
                        "Native vector training requires positive option '" + key + "'.");
            }
            return parsed;
        }
    }

    /**
     * Serializes the full native global training model.
     *
     * <p>TODO: Implement this once paimon-vector-index provides the corresponding API and
     * implementation on {@link VectorIndexTraining}.
     */
    private static byte[] serializeVectorIndexTraining(
            VectorIndexTraining training, Map<String, String> options) throws IOException {
        throw unsupportedNativeApi("VectorIndexTraining.serialize");
    }

    /**
     * Assigns one build-side vector to its closest IVF centroid.
     *
     * <p>TODO: Implement this once paimon-vector-index provides the corresponding API and
     * implementation on {@link VectorIndexTraining}.
     */
    private static int assignCentroid(
            VectorIndexTraining training, float[] vector, Map<String, String> options) {
        throw unsupportedNativeApi("VectorIndexTraining.assignCentroid");
    }

    /**
     * Selects centroid shards which may contain nearest neighbours for a query vector.
     *
     * <p>TODO: Implement this once paimon-vector-index provides the corresponding API and
     * implementation on {@link VectorIndexTraining}.
     */
    private static int[] findNearestCentroids(
            VectorIndexTraining training,
            float[] queryVector,
            int nprobe,
            Map<String, String> options) {
        throw unsupportedNativeApi("VectorIndexTraining.findNearestCentroids");
    }

    /**
     * Creates a native writer for one centroid shard from a global training model.
     *
     * <p>TODO: Implement this once paimon-vector-index provides the corresponding API and
     * implementation on {@link VectorIndexWriter}.
     */
    private static VectorIndexWriter createVectorIndexWriter(
            VectorIndexTraining training, int centroid, Map<String, String> options) {
        throw unsupportedNativeApi("VectorIndexWriter.createCentroidShardWriter");
    }

    /**
     * Adds a batch of absolute row ids and vectors to a centroid shard writer.
     *
     * <p>TODO: Implement this once paimon-vector-index provides the corresponding API and
     * implementation on {@link VectorIndexWriter}.
     */
    private static void addCentroidVectors(
            String indexType,
            VectorIndexWriter writer,
            long[] ids,
            float[] vectors,
            int vectorCount,
            Map<String, String> options) {
        throw unsupportedNativeApi("VectorIndexWriter.addCentroidVectors for " + indexType);
    }

    /**
     * Loads a persisted native global training model.
     *
     * <p>TODO: Implement this once paimon-vector-index provides the corresponding API and
     * implementation on {@link VectorIndexTraining}.
     */
    private static VectorIndexTraining deserializeVectorIndexTraining(
            byte[] payload, Map<String, String> options) throws IOException {
        throw unsupportedNativeApi("VectorIndexTraining.deserialize");
    }

    private static UnsupportedOperationException unsupportedNativeApi(String api) {
        return new UnsupportedOperationException(
                "Current paimon-vector-index Java binding does not support centroid IVF sharding. "
                        + "Waiting for native API: "
                        + api);
    }

    private static byte[] readFully(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }
}
