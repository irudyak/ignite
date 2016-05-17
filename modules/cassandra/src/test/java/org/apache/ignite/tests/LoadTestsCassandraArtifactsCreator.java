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

import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;

import java.util.LinkedList;
import java.util.List;

/**
 * Recreates all required Cassandra database objects (keyspace, table, indexes) for load tests
 */
public class LoadTestsCassandraArtifactsCreator {
    /**
     * Recreates Cassandra artifacts required for load tests
     * @param args not used
     */
    public static void main(String[] args) {
        System.out.println("Recreating Cassandra artifacts (keyspace, table, indexes) for load tests");

        KeyValuePersistenceSettings perSettings =
                new KeyValuePersistenceSettings(TestsHelper.getLoadTestsPersistenceSettings());

        System.out.println("Dropping test keyspace: " + perSettings.getKeyspace());

        try {
            CassandraHelper.dropTestKeyspaces();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to drop test keyspace: " + perSettings.getKeyspace(), e);
        }

        System.out.println("Test keyspace '" + perSettings.getKeyspace() + "' was successfully dropped");

        System.out.println("Creating test keyspace: " + perSettings.getKeyspace());

        try {
            CassandraHelper.executeWithAdminCredentials(perSettings.getKeyspaceDDLStatement());
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create test keyspace: " + perSettings.getKeyspace(), e);
        }

        System.out.println("Test keyspace '" + perSettings.getKeyspace() + "' was successfully created");

        System.out.println("Creating test table: " + perSettings.getTable());

        try {
            CassandraHelper.executeWithAdminCredentials(perSettings.getTableDDLStatement());
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create test table: " + perSettings.getTableDDLStatement(), e);
        }

        System.out.println("Test table '" + perSettings.getTableDDLStatement() + "' was successfully created");

        List<String> statements = perSettings.getIndexDDLStatements();
        if (statements == null)
            statements = new LinkedList<>();

        for (String statement : statements) {
            System.out.println("Creating test table index:");
            System.out.println(statement);

            try {
                CassandraHelper.executeWithAdminCredentials(statement);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to create test table index", e);
            }

            System.out.println("Test table index was successfully created");
        }

        System.out.println("All required Cassandra artifacts were successfully recreated");
    }
}
