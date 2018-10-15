package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class MergingDataIndexModel {
    String index_name;
    String index_field;
    List<String> related_index_name;
    List<String> related_index_field;

    public String getIndex_name() {
        return index_name;
    }
    public void setIndex_name(String index_name) {
        this.index_name = index_name;
    }
    public String getIndex_field() {
        return index_field;
    }
    public void setIndex_field(String index_field) {
        this.index_field = index_field;
    }
    public List<String> getRelated_index_name() {
        return related_index_name;
    }
    public void setRelated_index_name(List<String> related_index_name) {
        this.related_index_name = related_index_name;
    }
    public List<String> getRelated_index_field() {
        return related_index_field;
    }
    public void setRelated_index_field(List<String> related_index_field) {
        this.related_index_field = related_index_field;
    }
}
