package com.netflix.hollow.api.producer;

import static com.netflix.hollow.core.schema.HollowObjectSchema.FieldType.INT;
import static com.netflix.hollow.core.schema.HollowObjectSchema.FieldType.LONG;
import static com.netflix.hollow.core.schema.HollowObjectSchema.FieldType.REFERENCE;
import static com.netflix.hollow.core.schema.HollowObjectSchema.FieldType.STRING;

import java.util.ArrayDeque;
import java.util.Deque;

import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowObjectSchema.FieldType;
import com.netflix.hollow.core.write.HollowObjectTypeWriteState;
import com.netflix.hollow.core.write.HollowObjectWriteRecord;
import com.netflix.hollow.core.write.HollowWriteStateEngine;

public final class ObjectRecords implements Records {

    private final HollowWriteStateEngine writeEngine;
    private final String schemaName;
    private final Deque<ObjectRecords.Field> fields;

    private HollowObjectWriteRecord record = null;

    ObjectRecords(HollowWriteStateEngine writeEngine, String schemaName) {
        this.writeEngine = writeEngine;
        this.schemaName = schemaName;
        this.fields = new ArrayDeque<>();
    }

    @Override
    public Records defineInt(String fieldName) {
        if(record!=null) throw new CommittedSchemaException(schemaName, fieldName);
        fields.addLast(new Field(fieldName, INT));
        return this;
    }

    @Override
    public Records defineLong(String fieldName) {
        if(record!=null) throw new CommittedSchemaException(schemaName, fieldName);
        fields.addLast(new Field(fieldName, LONG));
        return this;
    }

    @Override
    public Records defineString(String fieldName) {
        if(record!=null) throw new CommittedSchemaException(schemaName, fieldName);
        fields.addLast(new Field(fieldName, STRING));
        return this;
    }

    @Override
    public Records defineReference(String fieldName, String referencedType) {
        if(record!=null) throw new CommittedSchemaException(schemaName, fieldName);
        fields.addLast(new Field(fieldName, REFERENCE, referencedType));
        return this;
    }

    @Override
    public Records setInt(String fieldName, int value) {
        if(record == null) throw new UncommittedSchemaException(schemaName);
        record.setInt(fieldName, value);
        return this;
    }

    @Override
    public Records setLong(String fieldName, long value) {
        if(record == null) throw new UncommittedSchemaException(schemaName);
        record.setLong(fieldName, value);
        return this;
    }

    @Override
    public Records setString(String fieldName, String value) {
        if(record == null) throw new UncommittedSchemaException(schemaName);
        record.setString(fieldName, value);
        return this;
    }

    @Override
    public Records setReference(String fieldName, int referencedOrdinal) {
        if(record == null) throw new UncommittedSchemaException(schemaName);
        record.setReference(fieldName, referencedOrdinal);
        return this;
    }

    @Override
    public Records commitSchema() {
        HollowObjectSchema schema = createSchema();
        writeEngine.addTypeState(new HollowObjectTypeWriteState(schema));
        this.record = new HollowObjectWriteRecord(schema);
        return this;
    }

    private HollowObjectSchema createSchema() {
        if(fields.isEmpty()) throw new IncompleteSchemaException(schemaName);

        HollowObjectSchema schema = new HollowObjectSchema(schemaName, fields.size());
        do {
            Field f = fields.removeFirst();
            schema.addField(f.name, f.type, f.referencedType, null);
        } while(!fields.isEmpty());
        return schema;
    }

    @Override
    public int add() {
        int ordinal = writeEngine.add(record.getSchema().getName(), record);
        record.reset();
        return ordinal;
    }

    private static final class Field {
        private final String name;
        private final FieldType type;
        private final String referencedType;

        public Field(String name, FieldType type) {
            this(name, type, null);
        }

        public Field(String name, FieldType type, String referencedType) {
            this.name = name;
            this.type = type;
            this.referencedType = referencedType;
        }
    }

}