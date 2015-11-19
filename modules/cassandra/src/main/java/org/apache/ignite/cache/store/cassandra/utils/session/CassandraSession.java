/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.cassandra.utils.session;

import java.io.Closeable;

/**
 * Wrapper around Cassandra driver session, to automatically handle:
 * <ul>
 *  <li>Keyspace and table absence exceptions</li>
 *  <li>Timeout exceptions</li>
 *  <li>Batch operations</li>
 * </ul>
 */
public interface CassandraSession extends Closeable {
    /** TODO IGNITE-1371: add comment */
    public <V> V execute(ExecutionAssistant<V> assistant);

    /** TODO IGNITE-1371: add comment */
    public <R, V> R execute(BatchExecutionAssistant<R, V> assistant, Iterable<? extends V> data);

    /** TODO IGNITE-1371: add comment */
    public void execute(BatchLoaderAssistant assistant);
}
