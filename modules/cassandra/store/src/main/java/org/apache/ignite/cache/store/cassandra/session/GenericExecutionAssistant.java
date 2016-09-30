package org.apache.ignite.cache.store.cassandra.session;

/**
 * Assistant used for execute query operations.
 */
public abstract class GenericExecutionAssistant<T> extends GenericAssistant implements ExecutionAssistant<T> {
    /**
     * Creates assistant with given settings.
     *
     * @param tableExistenceRequired is existing table needed
     * @param operationName name of operation
     */
    public GenericExecutionAssistant(boolean tableExistenceRequired, String operationName) {
        super(tableExistenceRequired, operationName);
    }
}
