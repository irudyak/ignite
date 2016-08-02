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

package org.apache.ignite.cache.store.cassandra;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.resources.SpringApplicationContextResource;

import javax.cache.configuration.Factory;

/**
 * Factory class to instantiate {@link CassandraCacheStore}.
 *
 * @param <K> Ignite cache key type
 * @param <V> Ignite cache value type
 */
public class CassandraCacheStoreFactory1<K, V> implements Factory<CassandraCacheStore1<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    private static CassandraCacheStore1 STORE;

    /** {@inheritDoc} */
    @Override public CassandraCacheStore1<K, V> create() {
        if (STORE == null)
            STORE = new CassandraCacheStore1<K, V>();

        return STORE;
    }
}
