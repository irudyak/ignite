package org.apache.ignite.cache.store.cassandra.session;

/**
 * Class which holds settings for specific operations on Cassandra tables.
 */
class GenericAssistant {
    /**
     * Specifies whether current operation requires table creation or not.
     */
    private final boolean tableExistenceRequired;

    /**
     * Specifies name of current operation.
     */
    private final String operationName;

    /**
     * Creates assistant with given settings.
     *
     * @param tableExistenceRequired is existing table needed
     * @param operationName name of operation
     */
    public GenericAssistant(boolean tableExistenceRequired, String operationName) {
        this.tableExistenceRequired = tableExistenceRequired;
        this.operationName = operationName;
    }

    /**
     * Is table existence required.
     *
     * @return returns true if this operation requires existing table. False otherwise.
     */
    public boolean tableExistenceRequired() {
        return tableExistenceRequired;
    }

    /**
     * Name of current operation.
     *
     * @return name.
     */
    public String operationName() {
        return operationName;
    }
}
