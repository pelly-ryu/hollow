package com.netflix.hollow.api.producer;

public interface Records {

    Records defineInt(String fieldName);
    Records defineLong(String fieldName);
    Records defineString(String fieldName);
    Records defineReference(String fieldName, String referencedType);

    Records setInt(String fieldName, int value);
    Records setLong(String fieldName, long value);
    Records setString(String fieldName, String value);
    Records setReference(String fieldName, int referencedOrdinal);

    Records commitSchema();

    int add();

}
