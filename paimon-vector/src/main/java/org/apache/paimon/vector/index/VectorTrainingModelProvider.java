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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** Provider for vector training model trainers and persisted model loading. */
public interface VectorTrainingModelProvider {

    /** Returns all vector index types supported by this provider. */
    default Set<String> supportedIndexTypes() {
        return Collections.singleton(identifier());
    }

    /** Returns the provider's primary index type identifier. */
    String identifier();

    VectorGlobalModelTrainer createTrainer(String indexType, Map<String, String> options);

    VectorTrainingModel loadModel(String indexType, Map<String, String> options, byte[] payload)
            throws IOException;
}
