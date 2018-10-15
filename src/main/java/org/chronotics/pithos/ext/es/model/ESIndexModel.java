package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESIndexModel {
    String index_name;
    List<String> index_types;

    public String getIndex_name() {
        return index_name;
    }

    public void setIndex_name(String index_name) {
        this.index_name = index_name;
    }

    public List<String> getIndex_types() {
        return index_types;
    }

    public void setIndex_types(List<String> index_types) {
        this.index_types = index_types;
    }
}
