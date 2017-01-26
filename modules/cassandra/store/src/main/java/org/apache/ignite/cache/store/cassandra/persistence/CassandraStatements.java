package org.apache.ignite.cache.store.cassandra.persistence;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to initialize and cache CQL statements which are used to manipulate data in Cassandra.
 */
public class CassandraStatements {
    /** CQL statement template to insert row into Cassandra table. */
    private final String writeStatementTempl;

    /** CQL statement template to delete row from Cassandra table. */
    private final String delStatementTempl;

    /** Ignite cache key/value persistence settings. */
    private final KeyValuePersistenceSettings persistenceSettings;

    /** Contains templates of load parameters to specific template of query. */
    private final Map<LoadStatementKey, String> loadStatementTemplates = new HashMap<>();

    /** Maps load parameters to CQL statements by table. */
    private final Map<LoadStatementKey, Map<String, String>> loadStatements = new HashMap<>();

    /** CQL statements to insert row into Cassandra table. */
    private volatile Map<String, String> writeStatements = new HashMap<>();

    /** CQL statements to delete row from Cassandra table. */
    private volatile Map<String, String> delStatements = new HashMap<>();

    /**
     * Constructs this statement holder.
     *
     * @param persistenceSettings persistence settings.
     */
    public CassandraStatements(KeyValuePersistenceSettings persistenceSettings) {
        this.persistenceSettings = persistenceSettings;
        writeStatementTempl = prepareWriteStatement();
        delStatementTempl = prepareDeleteStatement();
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
        return getLoadStatement(table, includeKeyFields, true, true);
    }

    /**
     * Returns CQL statement to select key/value fields from Cassandra table.
     *
     * @param table Table name.
     * @param includeKeyFields whether to include/exclude key fields from the returned row.
     *
     * @return CQL statement.
     */
    public String getLoadStatement(String table, boolean includeKeyFields, boolean includeValueFields,
        boolean includeFilter) {
        LoadStatementKey key = new LoadStatementKey(includeKeyFields, includeValueFields, includeFilter);
        String template = loadStatementTemplates.get(key);
        if (template == null) {
            synchronized (loadStatementTemplates) {
                loadStatementTemplates.put(key, prepareLoadTemplate(key));
                loadStatements.put(key, new HashMap<String, String>());
            }
            template = loadStatementTemplates.get(key);
        }
        return getStatement(table, template, loadStatements.get(key));
    }

    /**
     * Returns CQL statement to insert row into Cassandra table.
     *
     * @param table Table name.
     * @return CQL statement.
     */
    public String getWriteStatement(String table) {
        return getStatement(table, writeStatementTempl, writeStatements);
    }

    /**
     * Returns CQL statement to delete row from Cassandra table.
     *
     * @param table Table name.
     * @return CQL statement.
     */
    public String getDeleteStatement(String table) {
        return getStatement(table, delStatementTempl, delStatements);
    }

    /**
     * Returns specific statement by specified template.
     * @param table to get statement for.
     * @param template by which new statement will be created.
     * @param statements cache of already created statements.
     * @return CQL statement.
     */
    private String getStatement(final String table, final String template, final Map<String, String> statements) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (statements) {
            String st = statements.get(table);

            if (st == null) {
                st = String.format(template, table);
                statements.put(table, st);
            }

            return st;
        }
    }

    /**
     * Service method to prepare different CQL load statements.
     *
     * @param loadStatementKey key which specifies how load statement should be created.
     * @return CQL statement.
     */
    private String prepareLoadTemplate(LoadStatementKey loadStatementKey) {
        StringBuilder result = new StringBuilder();

        PersistenceSettings keySettings = persistenceSettings.getKeyPersistenceSettings();
        Collection<String> keyCols = keySettings.getTableColumns();
        if (loadStatementKey.includeKey) {
            boolean keyPojoStrategy = PersistenceStrategy.POJO == keySettings.getStrategy();
            for (String column : keyCols) {
                // omit calculated fields in load statement
                if (keyPojoStrategy && keySettings.getFieldByColumn(column).calculatedField())
                    continue;

                if (result.length() > 0)
                    result.append(", ");

                result.append("\"").append(column).append("\"");
            }
        }

        if (loadStatementKey.includeValue) {
            PersistenceSettings valueSettings = persistenceSettings.getValuePersistenceSettings();
            boolean pojoStrategy = PersistenceStrategy.POJO == valueSettings.getStrategy();
            Collection<String> valCols = valueSettings.getTableColumns();

            for (String column : valCols) {
                // omit calculated fields in load statement
                if (pojoStrategy && valueSettings.getFieldByColumn(column).calculatedField())
                    continue;

                if (result.length() > 0)
                    result.append(", ");

                result.append("\"").append(column).append("\"");

                if (!keyCols.contains(column))
                    result.append(", \"").append(column).append("\"");
            }
        }

        result.insert(0, "select ");
        StringBuilder statement = new StringBuilder();

        statement.append(" from \"");
        statement.append(persistenceSettings.getKeyspace());
        statement.append("\".\"%1$s\"");

        if (loadStatementKey.includeFilter) {
            statement.append(" where ");

            int i = 0;

            for (String column : keyCols) {
                if (i > 0)
                    statement.append(" and ");

                statement.append("\"").append(column).append("\"=?");
                i++;
            }
        }
        statement.append(";");

        return result + statement.toString();
    }

    /**
     * Service method to prepare CQL write statement.
     *
     * @return CQL write statement.
     */
    private String prepareWriteStatement() {
        Collection<String> cols = persistenceSettings.getTableColumns();

        StringBuilder colsList = new StringBuilder();
        StringBuilder questionsList = new StringBuilder();

        for (String column : cols) {
            if (colsList.length() != 0) {
                colsList.append(", ");
                questionsList.append(",");
            }

            colsList.append("\"").append(column).append("\"");
            questionsList.append("?");
        }

        String statement = "insert into \"" + persistenceSettings.getKeyspace() + "\".\"%1$s" +
            "\" (" + colsList + ") values (" + questionsList + ")";

        if (persistenceSettings.getTTL() != null)
            statement += " using ttl " + persistenceSettings.getTTL();

        return statement + ";";
    }

    /**
     * Service method to prepare CQL delete statement.
     *
     * @return CQL write statement.
     */
    private String prepareDeleteStatement() {
        Collection<String> cols = persistenceSettings.getKeyPersistenceSettings().getTableColumns();

        StringBuilder statement = new StringBuilder();

        for (String column : cols) {
            if (statement.length() != 0)
                statement.append(" and ");

            statement.append("\"").append(column).append("\"=?");
        }

        statement.append(";");

        return "delete from \"" + persistenceSettings.getKeyspace() + "\".\"%1$s\" where " + statement;
    }

    /**
     * Context class used to distinguish different parameters for load statements.
     */
    private static class LoadStatementKey {
        /**
         * Should include keys to query.
         */
        final boolean includeKey;

        /**
         * Should include values to query.
         */
        final boolean includeValue;

        /**
         * Should include where clause to query.
         */
        final boolean includeFilter;

        /**
         * Constructs key by given arguments.
         *
         * @param includeKey query key.
         * @param includeValue query value.
         * @param includeFilter query filter.
         */
        public LoadStatementKey(boolean includeKey, boolean includeValue, boolean includeFilter) {
            this.includeKey = includeKey;
            this.includeValue = includeValue;
            this.includeFilter = includeFilter;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LoadStatementKey key = (LoadStatementKey)o;

            if (includeKey != key.includeKey)
                return false;
            if (includeValue != key.includeValue)
                return false;
            return includeFilter == key.includeFilter;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = (includeKey ? 1 : 0);
            result = 31 * result + (includeValue ? 1 : 0);
            result = 31 * result + (includeFilter ? 1 : 0);
            return result;
        }
    }
}
