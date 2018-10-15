package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class MergingDataRequestModel {
    List<MergingDataIndexModel> index_fields;
    String unique_index_name;
    List<String> unique_field;
    List<String> indices;
    String new_index_name;
    String new_index_type;
    String date_type_field;

    public List<MergingDataIndexModel> getIndex_fields() {
        return index_fields;
    }
    public void setIndex_fields(List<MergingDataIndexModel> index_fields) {
        this.index_fields = index_fields;
    }
    public String getUnique_index_name() {
        return unique_index_name;
    }
    public void setUnique_index_name(String unique_index_name) {
        this.unique_index_name = unique_index_name;
    }
    public List<String> getUnique_field() {
        return unique_field;
    }
    public void setUnique_field(List<String> unique_field) {
        this.unique_field = unique_field;
    }
    public List<String> getIndices() {
        return indices;
    }
    public void setIndices(List<String> indices) {
        this.indices = indices;
    }
    public String getNew_index_name() {
        return new_index_name;
    }
    public void setNew_index_name(String new_index_name) {
        this.new_index_name = new_index_name;
    }
    public String getNew_index_type() {
        return new_index_type;
    }
    public void setNew_index_type(String new_index_type) {
        this.new_index_type = new_index_type;
    }
    public String getDate_type_field() {
        return date_type_field;
    }
    public void setDate_type_field(String date_type_field) {
        this.date_type_field = date_type_field;
    }
}
