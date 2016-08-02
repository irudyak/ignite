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
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * Unit tests for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store cache data into Cassandra tables
 */
public class TransactionsTestServer {
    /** */
    private static final Logger LOGGER = Logger.getLogger(TransactionsTestServer.class.getName());

    public static void setup() {
        if (CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.startEmbeddedCassandra(LOGGER);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to start embedded Cassandra instance", e);
            }
        }
    }

    public static void teardown() {
        if (CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.stopEmbeddedCassandra();
            }
            catch (Throwable e) {
                LOGGER.error("Failed to stop embedded Cassandra instance", e);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("1"))
            setup();

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/transactions/ignite-server-config-1.xml")) {
            while (true) {
                System.out.println(ManagementFactory.getRuntimeMXBean().getName());

                try {
                    Thread.sleep(10000);
                }
                catch (Throwable ignored) {
                }
            }
        }
    }
}
