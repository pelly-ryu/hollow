package com.netflix.hollow.api.producer;

final class IncompleteSchemaException extends IllegalStateException {
    private static final long serialVersionUID = 3618345763134455515L;

    public IncompleteSchemaException(String schemaName) {
        super("must have at least 1 field defined; schema="+schemaName);
    }
}
