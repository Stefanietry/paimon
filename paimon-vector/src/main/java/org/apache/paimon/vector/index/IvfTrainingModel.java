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

import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

/** Persistable IVF training model used for centroid routing and shard writing. */
public interface IvfTrainingModel extends VectorTrainingModel {

    /** Returns the IVF routing model used for build-side assignment and query-side routing. */
    IvfRoutingModel routingModel();

    /** Creates a physical index writer for vectors assigned to one centroid shard. */
    GlobalIndexSingleColumnWriter createCentroidShardIndexWriter(
            GlobalIndexFileWriter fileWriter, int centroid);
}
