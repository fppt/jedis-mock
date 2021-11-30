package com.github.fppt.jedismock.util;

public enum OperationCategory {
    BITMAPS("Bitmaps", "bitmap"),
    CONNECTION("Connection", "connection"),
    CLUSTER("Cluster", "cluster"),
    GEO("Geo", "geo"),
    HASHES("Hashes", "hash"),
    HYPERLOGLOG("HyperLogLog", "hyperloglog"),
    KEYS("Keys", "generic"),
    LISTS("Lists", "list"),
    PUBSUB("Pub/Sub", "pubsub"),
    SCRIPTING("Scripting", "scripting"),
    SERVER("Server", "server"),
    SETS("Sets", "set"),
    SORTEDSETS("Sorted Sets", "sorted_set"),
    STREAMS("Streams", "stream"),
    STRINGS("Strings", "string"),
    TRANSACTIONS("Transactions", "transactions");

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
