package me.escoffier.fluid.view;

import java.util.Map;

public class DocumentWithKey {

    private final String key;

    private final Map<String, Object> document;

    public DocumentWithKey(String key, Map<String, Object> document) {
        this.key = key;
        this.document = document;
    }

    public String key() {
        return key;
    }

    public Map<String, Object> document() {
        return document;
    }

}