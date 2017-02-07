package com.netflix.hollow.api.producer;

final class UncommittedSchemaException extends IllegalStateException {
    private static final long serialVersionUID = 6724855131031699601L;

    public UncommittedSchemaException(String schemaName) {
        super("defined but not committed; schema="+schemaName);
    }
}
