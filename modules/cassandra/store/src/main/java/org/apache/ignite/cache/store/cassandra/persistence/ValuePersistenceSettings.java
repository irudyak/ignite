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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Stores persistence settings for Ignite cache value
 */
public class ValuePersistenceSettings extends PersistenceSettings {
    /** XML element describing value field settings. */
    private static final String FIELD_ELEMENT = "field";

    /** Value fields. */
    private List<PojoField> fields = new LinkedList<>();

    /** Flag indicating if POJO fields were automatically discovered based on Java Bean class of the object. */
    private boolean fieldsAutoDiscovery = false;

    /**
     * Creates class instance from XML configuration.
     *
     * @param el XML element describing value persistence settings.
     */
    public ValuePersistenceSettings(Element el) {
        super(el);

        if (PersistenceStrategy.POJO != getStrategy()) {
            init();

            return;
        }

        NodeList nodes = el.getElementsByTagName(FIELD_ELEMENT);

        fields = detectPojoFields(nodes);

        if (fields.isEmpty())
            throw new IllegalStateException("Failed to initialize value fields for class '" + getJavaClass().getName() + "'");

        checkDuplicates(fields);

        init();
    }

    /**
     * @return List of value fields.
     */
    @Override public List<PojoField> getFields() {
        return fields == null ? null : Collections.unmodifiableList(fields);
    }

    /**
     * Flag indicating if POJO fields were automatically discovered based on Java Bean class of the object.
     *
     * @return true if fields were automatically discovered and false if not.
     */
    public boolean fieldsAutoDiscovery() {
        return fieldsAutoDiscovery;
    }

    /** {@inheritDoc} */
    @Override protected String defaultColumnName() {
        return "value";
    }

    /** {@inheritDoc} */
    @Override protected PojoField createPojoField(Element el, Class clazz) {
        return new PojoValueField(el, clazz);
    }

    /** {@inheritDoc} */
    @Override protected PojoField createPojoField(PojoFieldAccessor accessor) {
        return new PojoValueField(accessor);
    }
}
