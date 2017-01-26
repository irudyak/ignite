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

package org.apache.ignite.cache.store.cassandra.persistence;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

/**
 * Intermediate layer between persistent store (Cassandra) and Ignite cache key/value classes.
 * Handles  all the mappings to/from Java classes into Cassandra and responsible for all the details
 * of how Java objects should be written/loaded to/from Cassandra.
 */
public class PersistenceController {
    /** Ignite cache key/value persistence settings. */
    private final KeyValuePersistenceSettings persistenceSettings;

    /** List of key unique POJO fields (skipping aliases pointing to the same Cassandra table column). */
    private final List<PojoField> keyUniquePojoFields;

    /** List of value unique POJO fields (skipping aliases pointing to the same Cassandra table column). */
    private final List<PojoField> valUniquePojoFields;

    /** Holds CQL statements */
    private final CassandraStatements cassandraStatements;

    /**
     * Constructs persistence controller from Ignite cache persistence settings.
     *
     * @param settings persistence settings.
     */
    public PersistenceController(KeyValuePersistenceSettings settings) {
        if (settings == null)
            throw new IllegalArgumentException("Persistent settings can't be null");

        persistenceSettings = settings;

        cassandraStatements = new CassandraStatements(persistenceSettings);

        keyUniquePojoFields = settings.getKeyPersistenceSettings().cassandraUniqueFields();

        List<PojoField> _valUniquePojoFields = settings.getValuePersistenceSettings().cassandraUniqueFields();

        if (_valUniquePojoFields == null || _valUniquePojoFields.isEmpty()) {
            valUniquePojoFields = _valUniquePojoFields;

            return;
        }

        List<String> keyColumns = new LinkedList<>();

        if (keyUniquePojoFields == null)
            keyColumns.add(settings.getKeyPersistenceSettings().getColumn());
        else {
            for (PojoField field : keyUniquePojoFields)
                keyColumns.add(field.getColumn());
        }

        List<PojoField> fields = new LinkedList<>(_valUniquePojoFields);

        for (String column : keyColumns) {
            for (int i = 0; i < fields.size(); i++) {
                if (column.equals(fields.get(i).getColumn())) {
                    fields.remove(i);
                    break;
                }
            }
        }

        valUniquePojoFields = fields.isEmpty() ? null : Collections.unmodifiableList(fields);
    }

    /**
     * Returns Ignite cache persistence settings.
     *
     * @return persistence settings.
     */
    public KeyValuePersistenceSettings getPersistenceSettings() {
        return persistenceSettings;
    }

    /**
     * Returns CQL statement to insert row into Cassandra table.
     *
     * @param table Table name.
     * @return CQL statement.
     */
    public String getWriteStatement(String table) {
        return cassandraStatements.getWriteStatement(table);
    }

    /**
     * Returns CQL statement to delete row from Cassandra table.
     *
     * @param table Table name.
     * @return CQL statement.
     */
    public String getDeleteStatement(String table) {
        return cassandraStatements.getDeleteStatement(table);
    }

    /**
     * Returns CQL statement to select key/value fields from Cassandra table.
     *
     * @param table Table name.
     * @param includeKeyFields whether to include/exclude key fields from the returned row.
     *
     * @return CQL statement.
     */
    public String getLoadStatement(String table, boolean includeKeyFields) {
        return cassandraStatements.getLoadStatement(table, includeKeyFields);
    }

    /**
     * Returns CQL statement to select key/value fields from Cassandra table. Please make note that not all
     * possible combinations of parameters will return query.
     *
     * @param table Table name.
     * @param includeKeyFields whether to include/exclude key fields from the returned row.
     *
     * @return CQL statement.
     */
    public String getLoadStatement(String table, boolean includeKeyFields, boolean includeValueFields,
        boolean includeFilter) {
        return cassandraStatements.getLoadStatement(table, includeKeyFields, includeValueFields, includeFilter);
    }

    /**
     * Binds Ignite cache key object to {@link PreparedStatement}.
     *
     * @param statement statement to which key object should be bind.
     * @param key key object.
     *
     * @return statement with bounded key.
     */
    public BoundStatement bindKey(PreparedStatement statement, Object key) {
        PersistenceSettings settings = persistenceSettings.getKeyPersistenceSettings();

        Object[] values = PersistenceStrategy.POJO != settings.getStrategy() ?
            new Object[1] : new Object[keyUniquePojoFields.size()];

        bindValues(settings.getStrategy(), settings.getSerializer(), keyUniquePojoFields, key, values, 0);

        return statement.bind(values);
    }

    /**
     * Binds Ignite cache key and value object to {@link PreparedStatement}.
     *
     * @param statement statement to which key and value object should be bind.
     * @param key key object.
     * @param val value object.
     *
     * @return statement with bounded key and value.
     */
    public BoundStatement bindKeyValue(PreparedStatement statement, Object key, Object val) {
        Object[] values = new Object[persistenceSettings.getTableColumns().size()];

        PersistenceSettings keySettings = persistenceSettings.getKeyPersistenceSettings();
        PersistenceSettings valSettings = persistenceSettings.getValuePersistenceSettings();

        int offset = bindValues(keySettings.getStrategy(), keySettings.getSerializer(), keyUniquePojoFields, key, values, 0);
        bindValues(valSettings.getStrategy(), valSettings.getSerializer(), valUniquePojoFields, val, values, offset);

        return statement.bind(values);
    }

    /**
     * Builds Ignite cache key object from returned Cassandra table row.
     *
     * @param row Cassandra table row.
     *
     * @return key object.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Object buildKeyObject(Row row) {
        return buildObject(row, persistenceSettings.getKeyPersistenceSettings());
    }

    /**
     * Builds Ignite cache value object from Cassandra table row .
     *
     * @param row Cassandra table row.
     *
     * @return value object.
     */
    public Object buildValueObject(Row row) {
        return buildObject(row, persistenceSettings.getValuePersistenceSettings());
    }

    /**
     * Builds object from Cassandra table row.
     *
     * @param row Cassandra table row.
     * @param settings persistence settings to use.
     *
     * @return object.
     */
    private Object buildObject(Row row, PersistenceSettings settings) {
        if (row == null)
            return null;

        PersistenceStrategy stg = settings.getStrategy();

        Class clazz = settings.getJavaClass();
        String col = settings.getColumn();

        if (PersistenceStrategy.PRIMITIVE == stg)
            return buildObjectFromPrimitive(row, clazz, col);

        if (PersistenceStrategy.BLOB == stg)
            return buildObjectFromBlob(row, settings, col);

        return buildPojo(row, settings, clazz);
    }

    /**
     * Builds object from Cassandra table row serialized in blob.
     *
     * @param row Cassandra table row.
     * @param settings persistence settings to use.
     * @param col column.
     *
     * @return object.
     */
    protected Object buildObjectFromBlob(Row row, PersistenceSettings settings, String col) {
        return settings.getSerializer().deserialize(row.getBytes(col));
    }

    /**
     * Builds object from Cassandra table row serialized in primitive.
     *
     * @param row Cassandra table row.
     * @param clazz of primitive.
     * @param col column.
     *
     * @return object.
     */
    protected Object buildObjectFromPrimitive(Row row, Class clazz, String col) {
        return PropertyMappingHelper.getCassandraColumnValue(row, col, clazz, null);
    }

    /**
     * Builds object from Cassandra table row serialized in POJO.
     *
     * @param row Cassandra table row.
     * @param settings persistence settings to use.
     * @param clazz of POJO.
     *
     * @return object.
     */
    protected Object buildPojo(Row row, PersistenceSettings settings, Class clazz) {
        List<PojoField> fields = settings.getFields();

        Object obj;

        try {
            obj = clazz.newInstance();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to instantiate object of type '" + clazz.getName() + "' using reflection", e);
        }

        for (PojoField field : fields) {
            if (!field.calculatedField())
                field.setValueFromRow(row, obj, settings.getSerializer());
        }

        return obj;
    }

    /**
     * Extracts field values from POJO object, converts into Java types
     * which could be mapped to Cassandra types and stores them inside provided values
     * array starting from specified offset.
     *
     * @param stgy Persistence strategy to use.
     * @param serializer Serializer to use for BLOBs.
     * @param fields Fields who's values should be extracted.
     * @param obj Object instance who's field values should be extracted.
     * @param values Array to store values.
     * @param offset Offset starting from which to store fields values in the provided values array.
     *
     * @return next offset
     */
    private int bindValues(PersistenceStrategy stgy, Serializer serializer, List<PojoField> fields, Object obj,
                            Object[] values, int offset) {
        if (PersistenceStrategy.PRIMITIVE == stgy) {
            if (PropertyMappingHelper.getCassandraType(obj.getClass()) == null ||
                obj.getClass().equals(ByteBuffer.class) || obj instanceof byte[]) {
                throw new IllegalArgumentException("Couldn't deserialize instance of class '" +
                    obj.getClass().getName() + "' using PRIMITIVE strategy. Please use BLOB strategy for this case.");
            }

            values[offset] = obj;

            return ++offset;
        }

        if (PersistenceStrategy.BLOB == stgy) {
            values[offset] = serializer.serialize(obj);

            return ++offset;
        }

        if (fields == null || fields.isEmpty())
            return offset;

        for (PojoField field : fields) {
            Object val = field.getValueFromObject(obj, serializer);

            if (val instanceof byte[])
                val = ByteBuffer.wrap((byte[]) val);

            values[offset] = val;

            offset++;
        }

        return offset;
    }
}
