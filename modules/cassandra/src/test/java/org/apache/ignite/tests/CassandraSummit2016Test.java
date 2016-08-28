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

package org.apache.ignite.tests;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.tests.pojos.Order;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.pojos.PersonId;
import org.apache.ignite.tests.pojos.Product;
import org.apache.ignite.tests.utils.CacheStoreHelper;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.util.Collection;
import java.util.Map;

/**
 * Unit tests for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store cache data into Cassandra tables
 */
public class CassandraSummit2016Test {
    /** */
    private static final Logger LOGGER = Logger.getLogger(CassandraSummit2016Test.class.getName());

    /** */
    @BeforeClass
    public static void setUpClass() {
        if (CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.startEmbeddedCassandra(LOGGER);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to start embedded Cassandra instance", e);
            }
        }

        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();

        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();

        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();

        LOGGER.info("Start tests execution");
    }

    /** */
    @AfterClass
    public static void tearDownClass() {
        try {
            CassandraHelper.dropTestKeyspaces();
        }
        finally {
            CassandraHelper.releaseCassandraResources();

            if (CassandraHelper.useEmbeddedCassandra()) {
                try {
                    CassandraHelper.stopEmbeddedCassandra();
                }
                catch (Throwable e) {
                    LOGGER.error("Failed to stop embedded Cassandra instance", e);
                }
            }
        }
    }

    /** */
    @Test
    public void pojoStrategyTest() {
        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/summit2016/ignite-config.xml")) {
            IgniteCache<Long, Product> productCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Product>("product"));
            IgniteCache<Long, Order> orderCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Order>("order"));

            for (int i = 0; i < 100; i++) {
                Product prod = TestsHelper.generateRandomProduct();
                productCache.put(prod.getId(), prod);
            }
        }
    }

    /** */
    @Test
    public void loadCacheTest() {
        Ignition.stopAll(true);

        LOGGER.info("Running loadCache test");

        LOGGER.info("Filling Cassandra table with test data");

        CacheStore store = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml"),
            CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<PersonId, Person>> entries = TestsHelper.generatePersonIdsPersonsEntries();

        store.writeAll(entries);

        LOGGER.info("Cassandra table filled with test data");

        LOGGER.info("Running loadCache test");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));
            int size = personCache3.size(CachePeekMode.ALL);

            LOGGER.info("Initial cache size " + size);

            LOGGER.info("Loading cache data from Cassandra table");

            personCache3.loadCache(null, new String[] {"select * from test1.pojo_test3 limit 3"});

            size = personCache3.size(CachePeekMode.ALL);
            if (size != 3) {
                throw new RuntimeException("Cache data was incorrectly loaded from Cassandra. " +
                    "Expected number of records is 3, but loaded number of records is " + size);
            }

            LOGGER.info("Cache data loaded from Cassandra table");
        }

        LOGGER.info("loadCache test passed");
    }
}
