package com.netflix.hollow.api.producer;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.BitSet;

import org.junit.Before;
import org.junit.Test;

import com.netflix.hollow.api.StateTransition;
import com.netflix.hollow.api.objects.generic.GenericHollowObject;
import com.netflix.hollow.core.memory.ThreadSafeBitSet;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.schema.HollowSchema;
import com.netflix.hollow.core.schema.HollowSchema.SchemaType;
import com.netflix.hollow.core.util.StateEngineRoundTripper;
import com.netflix.hollow.core.write.HollowWriteStateEngine;

public final class ObjectRecordsTest {

    private HollowWriteStateEngine writeEngine;
    private WriteState writeState;
    
    @Before
    public void setup() {
        writeEngine = new HollowWriteStateEngine();
        writeState = new WriteState(writeEngine, new StateTransition(13L, 14L));
    }

    @Test
    public void definingSchemas() {
        writeState.objectRecordsOfType("String")
            .defineString("value")
            .commitSchema();

        writeState.objectRecordsOfType("Movie")
            .defineLong("id")
            .defineReference("title", "String")
            .defineInt("releaseYear")
            .commitSchema();

        assertThat(writeEngine.getTypeState("String"), notNullValue());
        assertThat(writeEngine.getTypeState("Movie"), notNullValue());

        HollowSchema stringSchema = writeEngine.getOrderedTypeStates().get(0).getSchema();
        assertThat(stringSchema.getSchemaType(), equalTo(SchemaType.OBJECT));
        assertThat(stringSchema.getName(), equalTo("String"));

        HollowSchema movieSchema = writeEngine.getOrderedTypeStates().get(1).getSchema();
        assertThat(movieSchema.getSchemaType(), equalTo(SchemaType.OBJECT));
        assertThat(movieSchema.getName(), equalTo("Movie"));
    }

    @Test
    public void addingRecords() throws Exception {
        Records stringRecords = writeState.objectRecordsOfType("String");
        Records movieRecords = writeState.objectRecordsOfType("Movie");

        stringRecords
            .defineString("value")
            .commitSchema();
        movieRecords
            .defineLong("id")
            .defineReference("title", "String")
            .defineInt("releaseYear")
            .commitSchema();

        int titleOrdinal = stringRecords
                .setString("value", "The Matrix")
                .add();
        int movieOrdinal = movieRecords
                .setLong("id", 1)
                .setReference("title", titleOrdinal)
                .setInt("releaseYear", 1999)
                .add();

        HollowReadStateEngine readEngine = StateEngineRoundTripper.roundTripSnapshot(writeEngine);
        GenericHollowObject movie = new GenericHollowObject(readEngine, "Movie", movieOrdinal);
        assertThat(movie.getLong("id"), equalTo(1L));
        assertThat(movie.getObject("title").getString("value"), equalTo("The Matrix"));
        assertThat(movie.getInt("releaseYear"), equalTo(1999));
    }

    @Test
    public void addingRecordsOfSameType() throws Exception {
        Records actorRecords = writeState.objectRecordsOfType("Actor");

        actorRecords
            .defineLong("id")
            .defineString("name")
            .commitSchema();

        BitSet ordinals = new BitSet();
        long nextActorId = 1L;
        for(String name : asList("Carrie-Anne Moss", "Hugo Weaving", "Keanu Reeves", "Laurence Fishburne")) {
            int ordinal = actorRecords
                    .setLong("id", nextActorId++)
                    .setString("name", name)
                    .add();
            ordinals.set(ordinal);
        }

        ThreadSafeBitSet populatedOrdinals = writeState.getStateEngine().getTypeState("Actor").getPopulatedBitSet();

        assertThat(populatedOrdinals.cardinality(), equalTo(ordinals.cardinality()));
        for(int i = ordinals.nextSetBit(0); i > 0; i = ordinals.nextSetBit(i+1))
            assertThat("ordinal="+i, populatedOrdinals.get(i), equalTo(ordinals.get(i)));
    }

}
