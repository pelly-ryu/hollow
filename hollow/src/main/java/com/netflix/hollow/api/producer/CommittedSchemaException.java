package com.netflix.hollow.api.producer;

final class CommittedSchemaException extends IllegalStateException {
    private static final long serialVersionUID = -7062997226215869948L;

    public CommittedSchemaException(String schema, String field) {
        super(String.format("cannot define a field once committed; schema=%s field=%s", schema, field));
    }
}
