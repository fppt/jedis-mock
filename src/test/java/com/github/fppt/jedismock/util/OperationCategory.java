package com.github.fppt.jedismock.util;

public enum OperationCategory {
    ADMIN("Administration", "admin"),
    BITMAPS("Bitmaps", "bitmap"),
    CONNECTION("Connection", "connection"),
    GEO("Geo", "geo"),
    HASHES("Hashes", "hash"),
    HYPERLOGLOG("HyperLogLog", "hyperloglog"),
    KEYS("Keys", "keyspace"),
    LISTS("Lists", "list"),
    PUBSUB("Pub/Sub", "pubsub"),
    SCRIPTING("Scripting", "scripting"),
    SETS("Sets", "set"),
    SORTEDSETS("Sorted Sets", "sortedset"),
    STREAMS("Streams", "stream"),
    STRINGS("Strings", "string"),
    TRANSACTIONS("Transactions", "transaction");

    private final String name;
    private final String annotationName;

    OperationCategory(String name, String annotationName) {
        this.name = name;
        this.annotationName = annotationName;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getAnnotationName() {
        return annotationName;
    }
}
