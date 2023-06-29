package org.example;

public class Secrets {
    // this is the lazy way...
    public static final String KAFKA_CLUSTER_KEY = System.getenv("KAFKA_CLUSTER_KEY");
    public static final String KAFKA_CLUSTER_SECRET = System.getenv("KAFKA_CLUSTER_SECRET");

    public static final String SCHEMA_REGISTRY_KEY = "REPLACE_WITH_SCHEMA_REGISTRY_KEY";
    public static final String SCHEMA_REGISTRY_SECRET = "REPLACE_WITH_SCHEMA_REGISTRY_SECRET";

}
