package com.trivadis.kafka.claimcheck.storage;

public class StorageReference {
    private final String uri;

    public StorageReference(String uri) {
        this.uri = uri;
    }

    public String getURI() {
        return uri;
    }

}
