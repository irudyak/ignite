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
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.tests.pojos.ProductOrder;
import org.apache.ignite.tests.pojos.Product;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;
import org.junit.Test;

import javax.cache.Cache;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Unit tests for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store cache data into Cassandra tables
 */
public class CassandraSummit2016Test {

    private static final String SERVER_CONF = "org/apache/ignite/tests/persistence/summit2016/ignite-server-config.xml";
    //private static final String CLIENT_CONF = "org/apache/ignite/tests/persistence/summit2016/ignite-client-config.xml";
    private static final String CLIENT_CONF = "org/apache/ignite/tests/persistence/summit2016/ignite-remote-client-config.xml";

    /** */
    private static final Logger LOGGER = Logger.getLogger(CassandraSummit2016Test.class.getName());

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
    public void serverTest() {
        Random rand = new Random(System.currentTimeMillis());

        setUpClass();

        try (Ignite ignite = Ignition.start(SERVER_CONF)) {
            IgniteCache<Long, Product> productCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Product>("product"));
            IgniteCache<Long, ProductOrder> orderCache = ignite.getOrCreateCache(new CacheConfiguration<Long, ProductOrder>("order"));

            Map<Integer, Product> products = new HashMap<>();
/*
            for (int i = 1; i <= 10; i++) {
                Product prod = TestsHelper.generateRandomProduct();
                productCache.put(prod.getId(), prod);

                int index = products.size();
                products.put(index, prod);
            }

            for (int i = 1; i <= 100; i++) {
                Product prod = products.get(rand.nextInt(products.size()));
                ProductOrder order = TestsHelper.generateRandomOrder(prod, i * 100000);

                System.out.println("ORDER_MS: " + order.getDayMillisecond());

                orderCache.put(order.getId(), order);

                Calendar cl = Calendar.getInstance();

                prod = products.get(rand.nextInt(products.size()));
                order = TestsHelper.generateRandomOrder(prod, i * 10 , cl.get(Calendar.YEAR),
                        cl.get(Calendar.MONTH), cl.get(Calendar.DAY_OF_MONTH) - 1, cl.get(Calendar.HOUR));

                orderCache.put(order.getId(), order);
            }
*/
            while (true) {
                try {
                    Thread.sleep(10000);
                    System.out.println("SERVER IS RUNNING");
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Test
    public void serverLightTest() {
        try (Ignite ignite = Ignition.start(SERVER_CONF)) {

            while (true) {
                try {
                    Thread.sleep(10000);
                    System.out.println("SERVER IS RUNNING");
                } catch (Throwable ignored) {
                }
            }
        }
    }

    /** */
    @Test
    public void jdbcTest() {
        try {
            // Register JDBC driver.
            Class.forName("org.apache.ignite.IgniteJdbcDriver");

            // Open JDBC connection (cache name is not specified, which means that we use default cache).
            Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://file:///D:/Projects/ignite/modules/cassandra/src/test/resources/" + CLIENT_CONF);

            // Query names of all people.
            ResultSet rs = conn.createStatement().executeQuery("select * from \"product\".Product");

            while (rs.next()) {
                String str = rs.getString(1);
                System.out.println(str);
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Test
    public void warmup() {
        String query = "select * from summit2016.order_history where daymillisecond='08/07/2016/${ignite_partition}'";

        try (Ignite ignite = Ignition.start(CLIENT_CONF)) {
            IgniteCache<Long, ProductOrder> orderHistory = ignite.getOrCreateCache(new CacheConfiguration<Long, ProductOrder>("order_history"));

            long start = System.currentTimeMillis();

            orderHistory.loadCache(null, new String[] {query});

            long duration = (System.currentTimeMillis() - start) / 1000;

            System.out.println("--------------------------------------");
            System.out.println("Warmup duration: " + duration + " sec");
            System.out.println("--------------------------------------");
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clearCache() {
        try (Ignite ignite = Ignition.start(CLIENT_CONF)) {
            IgniteCache<Long, Product> products = ignite.getOrCreateCache(new CacheConfiguration<Long, Product>("product"));
            IgniteCache<Long, ProductOrder> orders = ignite.getOrCreateCache(new CacheConfiguration<Long, ProductOrder>("order"));
            IgniteCache<Long, ProductOrder> ordersHistory = ignite.getOrCreateCache(new CacheConfiguration<Long, ProductOrder>("order_history"));
            long start = System.currentTimeMillis();
            products.clear();
            orders.clear();
            ordersHistory.clear();
            long duration = (System.currentTimeMillis() - start) / 1000;
            System.out.println("--------------------------------------");
            System.out.println("Clean duration: " + duration + " sec");
            System.out.println("--------------------------------------");
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
