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

package org.apache.ignite.cache.store.cassandra.utils.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.utils.common.SystemHelper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Stores persistence settings for Ignite cache key and value
 */
public class KeyValuePersistenceSettings {
    /** Xml attribute specifying Cassandra keyspace to use. */
    private static final String KEYSPACE_ATTR = "keyspace";

    /** Xml attribute specifying Cassandra table to use. */
    private static final String TABLE_ATTR = "table";

    /** Xml attribute specifying ttl (time to leave) for rows inserted in Cassandra. */
    private static final String TTL_ATTR = "ttl";

    /** Root xml element containing persistence settings specification. */
    private static final String PERSISTENCE_NODE = "persistence";

    /** Xml element specifying Cassandra keyspace options. */
    private static final String KEYSPACE_OPTIONS_NODE = "keyspaceOptions";

    /** Xml element specifying Cassandra table options. */
    private static final String TABLE_OPTIONS_NODE = "tableOptions";

    /** Xml element specifying Ignite cache key persistence settings. */
    private static final String KEY_PERSISTENCE_NODE = "keyPersistence";

    /** Xml element specifying Ignite cache value persistence settings. */
    private static final String VALUE_PERSISTENCE_NODE = "valuePersistence";

    /** TTL (time to leave) for rows inserted into Cassandra table. */
    private Integer ttl;

    /** Cassandra keyspace. */
    private String keyspace;

    /** Cassandra table. */
    private String tbl;

    /** Cassandra table creation options. */
    private String tblOptions;

    /** Cassandra keyspace creation options. */
    private String keyspaceOptions = "replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3} " +
        "and durable_writes = true";

    /** Persistence settings for Ignite cache keys */
    private KeyPersistenceSettings keyPersistenceSettings;

    /** Persistence settings for Ignite cache values */
    private ValuePersistenceSettings valPersistenceSettings;

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settings string containing xml with persistence settings for Ignite cache key/value
     */
    @SuppressWarnings("UnusedDeclaration")
    public KeyValuePersistenceSettings(String settings) {
        init(settings);
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settingsRsrc resource containing xml with persistence settings for Ignite cache key/value
     */
    public KeyValuePersistenceSettings(Resource settingsRsrc) {
        init(loadSettings(settingsRsrc));
    }

    /**
     * Returns ttl to use for while inserting new rows into Cassandra table.
     *
     * @return ttl
     */
    public Integer getTTL() {
        return ttl;
    }

    /**
     * Returns Cassandra keyspace to use.
     *
     * @return keyspace.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Returns Cassandra table to use.
     *
     * @return table.
     */
    public String getTable() {
        return tbl;
    }

    /**
     * Returns full name of Cassandra table to use (including keyspace).
     *
     * @return full table name in format "keyspace.table".
     */
    public String getTableFullName()
    {
        return keyspace + "." + tbl;
    }

    /**
     * Returns persistence settings for Ignite cache keys.
     *
     * @return keys persistence settings.
     */
    public KeyPersistenceSettings getKeyPersistenceSettings() {
        return keyPersistenceSettings;
    }

    /**
     * Returns persistence settings for Ignite cache values.
     *
     * @return values persistence settings.
     */
    public ValuePersistenceSettings getValuePersistenceSettings() {
        return valPersistenceSettings;
    }

    /**
     * Returns list of POJO fields to be mapped to Cassandra table columns.
     *
     * @return POJO fields list
     */
    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getFields() {
        List<PojoField> fields = new LinkedList<>();

        for (PojoField field : keyPersistenceSettings.getFields())
            fields.add(field);

        for (PojoField field : valPersistenceSettings.getFields())
            fields.add(field);

        return fields;
    }

    /**
     * Returns list of Ignite cache key POJO fields to be mapped to Cassandra table columns.
     *
     * @return POJO fields list
     */
    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getKeyFields() {
        return keyPersistenceSettings.getFields();
    }

    /**
     * Returns list of Ignite cache value POJO fields to be mapped to Cassandra table columns.
     *
     * @return POJO fields list
     */
    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getValueFields() {
        return valPersistenceSettings.getFields();
    }

    /**
     * Returns DDL statement to create Cassandra keyspace.
     *
     * @return keyspace DDL statement.
     */
    public String getKeyspaceDDLStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("create keyspace if not exists ").append(keyspace);

        if (keyspaceOptions != null) {
            if (!keyspaceOptions.trim().toLowerCase().startsWith("with"))
                builder.append(" with");

            builder.append(" ").append(keyspaceOptions);
        }

        String statement = builder.toString().trim();

        if (!statement.endsWith(";"))
            statement += ";";

        return statement;
    }

    /**
     * Returns DDL statement to create Cassandra table.
     *
     * @return table DDL statement.
     */
    public String getTableDDLStatement() {
        String colsDDL = keyPersistenceSettings.getTableColumnsDDL() + ", " + valPersistenceSettings.getTableColumnsDDL();

        String primaryKeyDDL = keyPersistenceSettings.getPrimaryKeyDDL();

        String clusteringDDL = keyPersistenceSettings.getClusteringDDL();

        String optionsDDL = tblOptions != null && !tblOptions.trim().isEmpty() ? tblOptions.trim() : "";

        if (clusteringDDL != null && !clusteringDDL.isEmpty())
            optionsDDL = optionsDDL.isEmpty() ? clusteringDDL : optionsDDL + " and " + clusteringDDL;

        if (!optionsDDL.trim().isEmpty())
            optionsDDL = optionsDDL.trim().toLowerCase().startsWith("with") ? optionsDDL.trim() : "with " + optionsDDL.trim();

        StringBuilder builder = new StringBuilder();

        builder.append("create table if not exists ").append(keyspace).append(".").append(tbl);
        builder.append(" (").append(colsDDL).append(", ").append(primaryKeyDDL).append(")");

        if (!optionsDDL.isEmpty())
            builder.append(" ").append(optionsDDL);

        String tblDDL = builder.toString().trim();

        return tblDDL.endsWith(";") ? tblDDL : tblDDL + ";";
    }

    /**
     * Returns DDL statements to create Cassandra table secondary indexes.
     *
     * @return DDL statements to create secondary indexes.
     */
    public List<String> getIndexDDLStatements() {
        List<String> idxDDLs = new LinkedList<>();

        List<PojoField> fields = valPersistenceSettings.getFields();

        for (PojoField field : fields) {
            if (((PojoValueField)field).isIndexed())
                idxDDLs.add(((PojoValueField)field).getIndexDDL(keyspace, tbl));
        }

        return idxDDLs;
    }

    /**
     * Loads Ignite cache persistence settings from resource
     *
     * @param rsrc resource
     *
     * @return string containing xml with Ignite cache persistence settings
     */
    private String loadSettings(Resource rsrc) {
        StringBuilder settings = new StringBuilder();
        InputStream in;
        BufferedReader reader = null;

        try {
            in = rsrc.getInputStream();
        }
        catch (IOException e) {
            throw new IgniteException("Failed to get input stream for Cassandra persistence settings resource: " + rsrc, e);
        }

        try {
            reader = new BufferedReader(new InputStreamReader(in));

            String line = reader.readLine();

            while (line != null) {
                if (settings.length() != 0)
                    settings.append(SystemHelper.LINE_SEPARATOR);

                settings.append(line);

                line = reader.readLine();
            }
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to read input stream for Cassandra persistence settings resource: " + rsrc, e);
        }
        finally {
            U.closeQuiet(reader);
            U.closeQuiet(in);
        }

        return settings.toString();
    }

    /**
     * @param elem Element with data.
     * @param attr Attribute name.
     * @return Numeric value for specified attribute.
     */
    private int extractIntAttribute(Element elem, String attr) {
        String val = elem.getAttribute(attr).trim();

        try {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Incorrect value '" + val + "' specified for '" + attr + "' attribute");
        }
    }

    /**
     * Initializes persistence settings from xml string
     *
     * @param settings xml string containing Ignite cache persistence settings configuration
     */
    @SuppressWarnings("IfCanBeSwitch")
    private void init(String settings) {
        Document doc;

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(new InputSource(new StringReader(settings)));
        }
        catch (Throwable e) {
            throw new IllegalArgumentException("Failed to parse persistence settings:" +
                SystemHelper.LINE_SEPARATOR + settings, e);
        }

        Element root = doc.getDocumentElement();

        if (!PERSISTENCE_NODE.equals(root.getNodeName())) {
            throw new IllegalArgumentException("Incorrect persistence settings specified. Root XML element " +
                "should be 'persistence'");
        }

        if (!root.hasAttribute(KEYSPACE_ATTR)) {
            throw new IllegalArgumentException("Incorrect persistence settings '" + KEYSPACE_ATTR + "' attribute " +
                "should be specified");
        }

        if (!root.hasAttribute(TABLE_ATTR)) {
            throw new IllegalArgumentException("Incorrect persistence settings '" + TABLE_ATTR + "' attribute " +
                "should be specified");
        }

        keyspace = root.getAttribute(KEYSPACE_ATTR).trim();
        tbl = root.getAttribute(TABLE_ATTR).trim();

        if (root.hasAttribute(TTL_ATTR))
            ttl = extractIntAttribute(root, TTL_ATTR);

        if (!root.hasChildNodes()) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no key and value persistence settings specified");
        }

        NodeList children = root.getChildNodes();
        int cnt = children.getLength();

        for (int i = 0; i < cnt; i++) {
            Node node = children.item(i);

            if (node.getNodeType() != Node.ELEMENT_NODE)
                continue;

            Element el = (Element)node;
            String nodeName = el.getNodeName();

            if (nodeName.equals(TABLE_OPTIONS_NODE)) {
                tblOptions = el.getTextContent();
                tblOptions = tblOptions.replace("\n", " ").replace("\r", "");
            }
            else if (nodeName.equals(KEYSPACE_OPTIONS_NODE)) {
                keyspaceOptions = el.getTextContent();
                keyspaceOptions = keyspaceOptions.replace("\n", " ").replace("\r", "");
            }
            else if (nodeName.equals(KEY_PERSISTENCE_NODE))
                keyPersistenceSettings = new KeyPersistenceSettings(el);
            else if (nodeName.equals(VALUE_PERSISTENCE_NODE))
                valPersistenceSettings = new ValuePersistenceSettings(el);
        }

        if (keyPersistenceSettings == null) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no key persistence settings specified");
        }

        if (valPersistenceSettings == null) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no value persistence settings specified");
        }

        List<PojoField> keyFields = keyPersistenceSettings.getFields();
        List<PojoField> valFields = valPersistenceSettings.getFields();

        if (PersistenceStrategy.POJO.equals(keyPersistenceSettings.getStrategy()) &&
            (keyFields == null || keyFields.isEmpty())) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no key fields found");
        }

        if (PersistenceStrategy.POJO.equals(valPersistenceSettings.getStrategy()) &&
            (valFields == null || valFields.isEmpty())) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no value fields found");
        }

        if (keyFields == null || keyFields.isEmpty() || valFields == null || valFields.isEmpty())
            return;

        for (PojoField keyField : keyFields) {
            for (PojoField valField : valFields) {
                if (keyField.getColumn().equals(valField.getColumn())) {
                    throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                        " key column '" + keyField.getColumn() + "' also specified as a value column");
                }
            }
        }
    }
}
